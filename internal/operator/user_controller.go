// Package operator implements the in-broker controller-runtime reconciler for
// the User CRD. Multiple Pods can run the manager concurrently; controller-
// runtime's K8s Lease leader election (`LeaderElection: true` in Run)
// ensures exactly one reconciles at a time.
package operator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
)

const (
	finalizerName     = "pgmqtt.io/user"
	credentialsSuffix = "-mqtt-credentials"
)

// UserReconciler reconciles User objects into rows in the users table and
// optionally generates a credentials Secret for clients to consume.
type UserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Pool   *pgxpool.Pool
	Logger *slog.Logger

	// Wire details for auto-generated Secrets — supplied via env so the
	// reconciler can patch stale host/port values.
	ServiceHost string // e.g. "pgmqtt.mqtt.svc.cluster.local"
	ServicePort int    // e.g. 1883
	WSPort      int    // e.g. 8083

	// BcryptCost overrides the cost used when hashing the User's password.
	// 0 falls back to bcrypt.DefaultCost (10).
	BcryptCost int
}

// SetupWithManager wires the controller for User into the manager.
func (r *UserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pgmqtt-user").
		For(&pgmqttv1alpha1.User{}).
		Owns(&corev1.Secret{}, builder.MatchEveryOwner).
		Complete(r)
}

// Reconcile implements controller-runtime.Reconciler.
//
// Ordering rationale: Secret reconcile happens BEFORE the bcrypt+PG write.
// Two managers can both think they hold the operator Lease during the
// ~15s lease-handoff window (controller-runtime's Lease default duration),
// and a freshly-created User CR with no pre-existing Secret means both
// managers will each generate their own random password via rand.Read.
// If the bcrypt+PG write happens before the Secret CreateOrUpdate, the
// last DB writer can disagree with the last K8s API writer:
//
//	t0  A.Get(secret) → NotFound; A generates password_A
//	t0  B.Get(secret) → NotFound; B generates password_B
//	t1  A bcrypt(A) → DB.UPSERT
//	t2  B bcrypt(B) → DB.UPSERT       (DB now has bcrypt(B))
//	t3  B Create(secret, B)            (Secret now has B)
//	t4  A Create(secret, A) → conflict → reload+retry
//	t5  A's MutateFn overwrites with A → Secret now has A
//	     final: Secret=A, DB=bcrypt(B) — auth fails forever
//
// Reordering the Secret reconcile to come first, *and* having the MutateFn
// preserve any existing data["password"] rather than always overwriting it,
// makes the password-that-wins the K8s API race the single source of truth.
// Both managers then re-Get the merged Secret and feed THAT password into
// bcrypt before writing PG, so the DB hash always matches the cleartext
// stored in the Secret regardless of which manager wrote last.
//
// +kubebuilder:rbac:groups=pgmqtt.io,resources=users,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgmqtt.io,resources=users/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=pgmqtt.io,resources=users/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
func (r *UserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("pgmqtt-user")

	var user pgmqttv1alpha1.User
	if err := r.Get(ctx, req.NamespacedName, &user); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !user.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &user)
	}

	if !controllerutil.ContainsFinalizer(&user, finalizerName) {
		controllerutil.AddFinalizer(&user, finalizerName)
		if err := r.Update(ctx, &user); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	username := user.Spec.Username
	if username == "" {
		username = user.Name
	}

	secret, password, err := r.resolveCredentialSecret(ctx, &user, username)
	if err != nil {
		setReady(&user, false, "SecretError", err.Error())
		_ = r.Status().Update(ctx, &user)
		return ctrl.Result{}, err
	}

	hash := sha256Hex(password)
	if user.Status.ObservedSecretHash == hash &&
		user.Status.CredentialsSecretRef != nil &&
		user.Status.CredentialsSecretRef.Name == secret.Name {
		setReady(&user, true, "Reconciled", "")
		return ctrl.Result{}, r.Status().Update(ctx, &user)
	}

	cost := r.BcryptCost
	if cost == 0 {
		cost = bcrypt.DefaultCost
	}
	bcryptHash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("bcrypt: %w", err)
	}

	if _, err := r.Pool.Exec(ctx, `
		INSERT INTO users(username, password_hash) VALUES($1, $2)
		ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash
	`, username, string(bcryptHash)); err != nil {
		setReady(&user, false, "DBError", err.Error())
		_ = r.Status().Update(ctx, &user)
		return ctrl.Result{}, err
	}

	user.Status.ObservedSecretHash = hash
	user.Status.CredentialsSecretRef = &corev1.LocalObjectReference{Name: secret.Name}
	setReady(&user, true, "Reconciled", "")
	if err := r.Status().Update(ctx, &user); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("user upserted", "username", username, "secret", secret.Name)
	return ctrl.Result{}, nil
}

func (r *UserReconciler) reconcileDelete(ctx context.Context, user *pgmqttv1alpha1.User) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(user, finalizerName) {
		return ctrl.Result{}, nil
	}
	username := user.Spec.Username
	if username == "" {
		username = user.Name
	}
	if _, err := r.Pool.Exec(ctx, `DELETE FROM users WHERE username=$1`, username); err != nil {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(user, finalizerName)
	return ctrl.Result{}, r.Update(ctx, user)
}

// resolveCredentialSecret returns the Secret referenced by spec.PasswordSecretRef,
// or reconciles a `<name>-mqtt-credentials` Secret in the User's namespace.
// Returns the Secret and the raw password value.
//
// For the auto-generated case the password-that-wins is determined by the
// K8s API write race inside CreateOrUpdate, NOT by which manager generated
// the candidate first. Concretely: the MutateFn preserves any existing
// data["password"] rather than overwriting it, so the FIRST manager to
// successfully Create the Secret seeds the password value; subsequent
// peers re-Get the merged Secret to read whatever password landed. This
// closes the lease-handoff divergence between Secret-cleartext and
// PG-bcrypt-hash described on the Reconcile docstring.
func (r *UserReconciler) resolveCredentialSecret(ctx context.Context, user *pgmqttv1alpha1.User, username string) (*corev1.Secret, string, error) {
	if user.Spec.PasswordSecretRef != nil {
		ref := user.Spec.PasswordSecretRef
		var sec corev1.Secret
		if err := r.Get(ctx, client.ObjectKey{Namespace: user.Namespace, Name: ref.Name}, &sec); err != nil {
			return nil, "", fmt.Errorf("get secret %s/%s: %w", user.Namespace, ref.Name, err)
		}
		key := ref.Key
		if key == "" {
			key = "password"
		}
		password, ok := sec.Data[key]
		if !ok {
			return nil, "", fmt.Errorf("secret %s/%s missing key %s", user.Namespace, ref.Name, key)
		}
		return &sec, string(password), nil
	}

	name := user.Name + credentialsSuffix

	// Generate a candidate password. We only use it if the Secret didn't
	// exist or its data["password"] was empty — see MutateFn below.
	candidate, err := generatePassword()
	if err != nil {
		return nil, "", err
	}

	host := r.ServiceHost
	if host == "" {
		host = "pgmqtt"
	}
	port := r.ServicePort
	if port == 0 {
		port = 1883
	}
	wsPort := r.WSPort
	if wsPort == 0 {
		wsPort = 8083
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: user.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, desired, func() error {
		desired.Type = corev1.SecretTypeOpaque
		if desired.Data == nil {
			desired.Data = map[string][]byte{}
		}
		// Preserve any existing password so two concurrent reconciles
		// during a lease-handoff converge on the same value: whichever
		// manager Created the Secret first wins, and the loser's
		// CreateOrUpdate retry sees data["password"] populated and keeps
		// it instead of forcing its own candidate.
		password := desired.Data["password"]
		if len(password) == 0 {
			password = []byte(candidate)
		}
		desired.Data["username"] = []byte(username)
		desired.Data["password"] = password
		desired.Data["host"] = []byte(host)
		desired.Data["port"] = []byte(fmt.Sprintf("%d", port))
		desired.Data["ws-port"] = []byte(fmt.Sprintf("%d", wsPort))
		desired.Data["uri"] = []byte(fmt.Sprintf("mqtt://%s:%s@%s:%d",
			url.PathEscape(username), url.PathEscape(string(password)), host, port))
		desired.Data["ws-uri"] = []byte(fmt.Sprintf("ws://%s:%s@%s:%d/mqtt",
			url.PathEscape(username), url.PathEscape(string(password)), host, wsPort))
		return controllerutil.SetControllerReference(user, desired, r.Scheme)
	})
	if err != nil {
		return nil, "", err
	}
	r.Logger.Debug("credentials secret reconciled", "op", op, "name", name)

	// Re-Get after the merge so we observe the password that "won" the
	// CreateOrUpdate race. Without this, a concurrent peer manager could
	// have written a different password between our MutateFn run and our
	// caller's bcrypt+PG write — leading to Secret/DB divergence.
	var resolved corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: user.Namespace, Name: name}, &resolved); err != nil {
		return nil, "", fmt.Errorf("re-get secret %s/%s: %w", user.Namespace, name, err)
	}
	pw, ok := resolved.Data["password"]
	if !ok || len(pw) == 0 {
		return nil, "", fmt.Errorf("secret %s/%s missing password after reconcile", user.Namespace, name)
	}
	return &resolved, string(pw), nil
}

// generatePassword returns a base64-url-encoded 24-byte random password.
func generatePassword() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func setReady(user *pgmqttv1alpha1.User, ready bool, reason, msg string) {
	cond := metav1.Condition{
		Type:               "Ready",
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            msg,
	}
	if ready {
		cond.Status = metav1.ConditionTrue
	} else {
		cond.Status = metav1.ConditionFalse
	}
	for i, c := range user.Status.Conditions {
		if c.Type == "Ready" {
			if c.Status == cond.Status && c.Reason == cond.Reason && c.Message == cond.Message {
				return
			}
			user.Status.Conditions[i] = cond
			return
		}
	}
	user.Status.Conditions = append(user.Status.Conditions, cond)
}
