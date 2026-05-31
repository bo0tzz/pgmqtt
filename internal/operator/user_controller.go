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
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
	// Optional TLS endpoint for the User CR's Secret. When TLSHost is
	// non-empty the operator emits an mqtts:// URI alongside the plain
	// mqtt:// one. Operators wanting to push apps to TLS-only set the
	// matching listener up out-of-band (ingress-nginx, HAProxy, etc.).
	TLSHost string
	TLSPort int    // e.g. 8883
	WSSPort int    // e.g. 8443

	// BcryptCost overrides the cost used when hashing the User's password.
	// 0 falls back to bcrypt.DefaultCost (10).
	BcryptCost int
}

// SetupWithManager wires the controller for User into the manager.
//
// The Owns() relationship enqueues reconciles when an auto-generated
// credentials Secret (the kind the operator creates with an
// OwnerReference back to the User) changes. BYO Secrets — those
// supplied by the cluster operator via spec.PasswordSecretRef and
// pointing at a Secret they manage themselves — carry NO owner ref,
// so Owns() will never fire for them.
//
// Without the Watches() clause below, a rotated BYO Secret would not
// trigger a reconcile until either the User CR itself changed or the
// controller-runtime resync period fired (default 10h). The Postgres
// users table would carry the stale bcrypt for that whole window,
// which is far too slow for any sensible rotation flow.
//
// The map function lists User CRs in the changed Secret's namespace
// (cheap — served from the informer cache) and enqueues the ones
// whose spec.PasswordSecretRef.Name matches the Secret. Non-matching
// Secret events return an empty slice, keeping the cost of cluster-
// wide Secret churn bounded to a single namespaced List per event.
func (r *UserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pgmqtt-user").
		For(&pgmqttv1alpha1.User{}).
		Owns(&corev1.Secret{}, builder.MatchEveryOwner).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.usersForSecret),
		).
		Complete(r)
}

// usersForSecret maps a Secret change to the set of User CRs that
// reference that Secret via spec.PasswordSecretRef. Used by the
// Watches() clause in SetupWithManager so BYO Secret rotations
// propagate to Postgres within one reconcile tick instead of waiting
// for the controller-runtime resync period (default 10h).
//
// Performance: the function runs on every Secret create/update/delete
// in every namespace the manager's cache covers. The namespaced User
// List is served from the informer cache and is cheap; the early
// return for empty UserList keeps non-pgmqtt Secret churn essentially
// free. Returning nil for non-matching events is required — handler
// machinery treats a nil/empty slice as "do not enqueue".
func (r *UserReconciler) usersForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}
	var users pgmqttv1alpha1.UserList
	if err := r.List(ctx, &users, client.InNamespace(secret.GetNamespace())); err != nil {
		// Logging here is best-effort: the next reconcile (or the
		// resync) will eventually re-process. We don't want to fail
		// the watch loop on a transient cache miss.
		log.FromContext(ctx).WithName("pgmqtt-user").Error(err,
			"usersForSecret: list users in namespace failed",
			"namespace", secret.GetNamespace(), "secret", secret.GetName())
		return nil
	}
	var reqs []reconcile.Request
	for i := range users.Items {
		u := &users.Items[i]
		if u.Spec.PasswordSecretRef == nil {
			continue
		}
		if u.Spec.PasswordSecretRef.Name != secret.GetName() {
			continue
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: client.ObjectKey{
				Namespace: u.Namespace,
				Name:      u.Name,
			},
		})
	}
	return reqs
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

	// Rename handling: if spec.username has been edited since the last
	// successful reconcile, the row keyed by the OLD username must be
	// removed — otherwise the old credentials remain valid for auth and
	// the old username slot stays occupied even after the slot is
	// logically free. status.observedUsername is the authoritative
	// pointer to the row we last wrote, so we delete that row first and
	// fall through to the upsert which writes the new one.
	//
	// Idempotency: the DELETE is a no-op when the row is already gone,
	// so a mid-reconcile failure that prevents the status update is
	// safe — the next pass observes the same observedUsername, re-runs
	// the (no-op) DELETE, and tries the upsert again.
	renamed := user.Status.ObservedUsername != "" && user.Status.ObservedUsername != username
	if renamed {
		if _, err := r.Pool.Exec(ctx, `DELETE FROM users WHERE username=$1`, user.Status.ObservedUsername); err != nil {
			setReady(&user, false, "DBError", scrubReason(err))
			logStatusUpdateError(logger, r.Status().Update(ctx, &user),
				"rename-cleanup DBError", err)
			return ctrl.Result{}, err
		}
	}

	secret, password, err := r.resolveCredentialSecret(ctx, &user, username)
	if err != nil {
		setReady(&user, false, "SecretError", scrubReason(err))
		logStatusUpdateError(logger, r.Status().Update(ctx, &user),
			"resolveCredentialSecret SecretError", err)
		return ctrl.Result{}, err
	}

	cost := r.BcryptCost
	if cost == 0 {
		cost = bcrypt.DefaultCost
	}

	// Decide whether the short-circuit applies. The cleartext-unchanged
	// branch fires when ObservedSecretHash matches AND the stored bcrypt
	// row is at-or-above the configured cost. Bumping operator.bcryptCost
	// must NOT leave existing rows on the old cost forever — see
	// pgmqtt_user_rehash_total{reason="cost_bump"} for rollout tracking.
	//
	// On a rename we skip the short-circuit entirely: even when the
	// password is unchanged the new-username row may not yet exist in PG
	// (storedBcryptCost would return 0 → cost-bump branch), but more
	// importantly we want the upsert path to run unconditionally so the
	// new row lands and status.observedUsername advances.
	//
	// Requiring ObservedUsername == username (NOT just !renamed) also
	// handles the upgrade path: an old User CR reconciled by a pre-rename-
	// handling broker has ObservedSecretHash set but ObservedUsername
	// empty. The first reconcile on the new broker bypasses the short-
	// circuit, runs the (idempotent) upsert, and backfills
	// ObservedUsername so subsequent reconciles short-circuit normally.
	hash := sha256Hex(password)
	rehashReason := ""
	if user.Status.ObservedUsername == username &&
		user.Status.ObservedSecretHash == hash &&
		user.Status.CredentialsSecretRef != nil &&
		user.Status.CredentialsSecretRef.Name == secret.Name {
		// Cleartext is unchanged — still need to verify the stored hash
		// is at the configured cost. A miss here means an operator bumped
		// bcryptCost since the last reconcile; we re-bcrypt the same
		// password at the new cost.
		storedCost, err := r.storedBcryptCost(ctx, username)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("read stored cost: %w", err)
		}
		if storedCost >= cost {
			setReady(&user, true, "Reconciled", "")
			return ctrl.Result{}, r.Status().Update(ctx, &user)
		}
		rehashReason = "cost_bump"
	} else {
		rehashReason = "rotation"
	}

	bcryptHash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("bcrypt: %w", err)
	}

	if _, err := r.Pool.Exec(ctx, `
		INSERT INTO users(username, password_hash) VALUES($1, $2)
		ON CONFLICT (username) DO UPDATE SET password_hash=EXCLUDED.password_hash
	`, username, string(bcryptHash)); err != nil {
		setReady(&user, false, "DBError", scrubReason(err))
		logStatusUpdateError(logger, r.Status().Update(ctx, &user),
			"users upsert DBError", err)
		return ctrl.Result{}, err
	}
	userRehashTotal.WithLabelValues(rehashReason).Inc()

	user.Status.ObservedSecretHash = hash
	user.Status.ObservedUsername = username
	user.Status.CredentialsSecretRef = &corev1.LocalObjectReference{Name: secret.Name}
	setReady(&user, true, "Reconciled", "")
	if err := r.Status().Update(ctx, &user); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("user upserted", "username", username, "secret", secret.Name, "reason", rehashReason)
	return ctrl.Result{}, nil
}

// storedBcryptCost returns the cost parameter encoded in the user row's
// password_hash, or 0 if the row does not exist (caller should treat as
// "needs initial hash"). Returns -1 only on a DB error so the caller can
// distinguish "no row" (proceed with rehash branch via the reason path)
// from "PG unavailable" (return error).
//
// The pgx ErrNoRows case is mapped to (0, nil) — the row will be inserted
// by the upsert below, so a stored cost of 0 reliably falls under any
// configured cost (which is at least bcrypt.MinCost = 4).
func (r *UserReconciler) storedBcryptCost(ctx context.Context, username string) (int, error) {
	var stored string
	err := r.Pool.QueryRow(ctx, `SELECT password_hash FROM users WHERE username=$1`, username).Scan(&stored)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return -1, err
	}
	c, err := bcrypt.Cost([]byte(stored))
	if err != nil {
		// Stored hash is unparseable (corrupted or non-bcrypt). Treat as
		// cost=0 so the rehash branch fires and replaces it.
		return 0, nil
	}
	return c, nil
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
		// TLS endpoints — populated only when an operator wires
		// TLSHost/TLSPort into the broker's env (helm
		// `operator.tlsHost` etc.). The plain mqtt:// URI stays in
		// the Secret either way for in-cluster traffic; mqtts://
		// gives consumer apps an opinionated path to the encrypted
		// listener so they don't fall back to plaintext by default.
		if r.TLSHost != "" {
			tlsPort := r.TLSPort
			if tlsPort == 0 {
				tlsPort = 8883
			}
			desired.Data["tls-host"] = []byte(r.TLSHost)
			desired.Data["tls-port"] = []byte(fmt.Sprintf("%d", tlsPort))
			desired.Data["mqtts-uri"] = []byte(fmt.Sprintf("mqtts://%s:%s@%s:%d",
				url.PathEscape(username), url.PathEscape(string(password)), r.TLSHost, tlsPort))
			if wssPort := r.WSSPort; wssPort > 0 {
				desired.Data["wss-port"] = []byte(fmt.Sprintf("%d", wssPort))
				desired.Data["wss-uri"] = []byte(fmt.Sprintf("wss://%s:%s@%s:%d/mqtt",
					url.PathEscape(username), url.PathEscape(string(password)), r.TLSHost, wssPort))
			}
		}
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

// scrubReason produces a User-CR-Status-safe summary of an error. We
// classify by SQLSTATE / common substrings rather than reflecting raw
// pgx error text — those messages can contain query fragments, schema
// names, and (when the connection string is in the error) plaintext
// credentials. CWE-209.
func scrubReason(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no rows"):
		return "not found"
	case strings.Contains(s, "duplicate key"),
		strings.Contains(s, "23505"):
		return "duplicate"
	case strings.Contains(s, "23502"):
		return "constraint violation"
	case strings.Contains(s, "context deadline exceeded"),
		strings.Contains(s, "i/o timeout"),
		strings.Contains(s, "connection refused"),
		strings.Contains(s, "connection reset"),
		strings.Contains(s, "EOF"):
		return "transient connectivity"
	case strings.Contains(s, "permission denied"),
		strings.Contains(s, "42501"):
		return "permission denied"
	default:
		return "internal error"
	}
}

// UsersForSecretForTest exposes the Watches map function so external
// tests can validate the BYO-Secret → User fan-out without spinning up
// a full controller-runtime manager. Production code paths go through
// the handler/EnqueueRequestsFromMapFunc wiring set up in
// SetupWithManager; this helper is only for unit-test assertions.
func (r *UserReconciler) UsersForSecretForTest(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.usersForSecret(ctx, obj)
}

// logStatusUpdateError surfaces a swallowed Status().Update() error as a
// WARN log alongside the underlying reconcile error. Callers still
// propagate the underlying reconcile error to controller-runtime (status-
// write failures are secondary), but losing the diagnostic was making
// lease-handoff RV conflicts and other transient status-write failures
// invisible to operators.
func logStatusUpdateError(logger logr.Logger, statusErr error, scope string, underlying error) {
	if statusErr == nil {
		return
	}
	logger.Info("status update failed; original reconcile error preserved",
		"scope", scope, "status_err", statusErr.Error(),
		"underlying_err", underlying.Error())
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
