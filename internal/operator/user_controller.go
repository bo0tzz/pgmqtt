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
// or generates a `<name>-mqtt-credentials` Secret in the User's namespace.
// Returns the Secret and the raw password value.
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
	var existing corev1.Secret
	err := r.Get(ctx, client.ObjectKey{Namespace: user.Namespace, Name: name}, &existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, "", err
	}
	password := ""
	if err == nil {
		// Secret already exists — preserve its password but refresh wire details.
		if pw, ok := existing.Data["password"]; ok {
			password = string(pw)
		}
	}
	if password == "" {
		raw := make([]byte, 24)
		if _, err := rand.Read(raw); err != nil {
			return nil, "", err
		}
		password = base64.RawURLEncoding.EncodeToString(raw)
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

	data := map[string][]byte{
		"username": []byte(username),
		"password": []byte(password),
		"host":     []byte(host),
		"port":     []byte(fmt.Sprintf("%d", port)),
		"ws-port":  []byte(fmt.Sprintf("%d", wsPort)),
		"uri":      []byte(fmt.Sprintf("mqtt://%s:%s@%s:%d", url.PathEscape(username), url.PathEscape(password), host, port)),
		"ws-uri":   []byte(fmt.Sprintf("ws://%s:%s@%s:%d/mqtt", url.PathEscape(username), url.PathEscape(password), host, wsPort)),
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: user.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
	}
	if err := controllerutil.SetControllerReference(user, desired, r.Scheme); err != nil {
		return nil, "", err
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, desired, func() error {
		desired.Type = corev1.SecretTypeOpaque
		if desired.Data == nil {
			desired.Data = map[string][]byte{}
		}
		for k, v := range data {
			desired.Data[k] = v
		}
		return controllerutil.SetControllerReference(user, desired, r.Scheme)
	})
	if err != nil {
		return nil, "", err
	}
	r.Logger.Debug("credentials secret reconciled", "op", op, "name", name)
	return desired, password, nil
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
