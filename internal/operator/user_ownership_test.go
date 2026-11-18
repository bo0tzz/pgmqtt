package operator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pgmqttv1alpha1 "github.com/bo0tzz/pgmqtt/api/v1alpha1"
	"github.com/bo0tzz/pgmqtt/internal/db/dbtest"
	"github.com/bo0tzz/pgmqtt/internal/operator"
)

func namedUser(namespace, name, username string) *pgmqttv1alpha1.User {
	return &pgmqttv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID("uid-" + namespace + "-" + name)},
		Spec:       pgmqttv1alpha1.UserSpec{Username: username},
	}
}

func generatedPassword(t *testing.T, cli client.Client, namespace, userName string) []byte {
	t.Helper()
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: namespace, Name: userName + "-mqtt-credentials"}
	if err := cli.Get(context.Background(), key, &sec); err != nil {
		t.Fatalf("get generated secret %s: %v", key, err)
	}
	return sec.Data["password"]
}

func rowOwner(t *testing.T, pool *pgxpool.Pool, username string) (namespace, name, uid string) {
	t.Helper()
	var ns, n, u *string
	if err := pool.QueryRow(context.Background(),
		`SELECT owner_namespace, owner_name, owner_uid FROM users WHERE username=$1`, username,
	).Scan(&ns, &n, &u); err != nil {
		t.Fatalf("read owner of %q: %v", username, err)
	}
	deref := func(s *string) string {
		if s == nil {
			return "<null>"
		}
		return *s
	}
	return deref(ns), deref(n), deref(u)
}

func assertOwner(t *testing.T, pool *pgxpool.Pool, username string, u *pgmqttv1alpha1.User) {
	t.Helper()
	ns, name, uid := rowOwner(t, pool, username)
	if ns != u.Namespace || name != u.Name || uid != string(u.UID) {
		t.Errorf("row %q owned by %s/%s (%s), want %s/%s (%s)", username, ns, name, uid, u.Namespace, u.Name, u.UID)
	}
}

func assertVerifies(t *testing.T, pool *pgxpool.Pool, username string, password []byte) {
	t.Helper()
	if err := bcrypt.CompareHashAndPassword([]byte(storedHash(t, pool, username)), password); err != nil {
		t.Errorf("row %q does not verify the expected password: %v", username, err)
	}
}

func deleteAndReconcile(t *testing.T, r *operator.UserReconciler, cli client.Client, u *pgmqttv1alpha1.User) {
	t.Helper()
	if err := cli.Delete(context.Background(), u); err != nil {
		t.Fatalf("delete %s/%s: %v", u.Namespace, u.Name, err)
	}
	if _, err := reconcileUser(r, u.Namespace, u.Name); err != nil {
		t.Fatalf("reconcile deletion of %s/%s: %v", u.Namespace, u.Name, err)
	}
	var gone pgmqttv1alpha1.User
	err := cli.Get(context.Background(), types.NamespacedName{Namespace: u.Namespace, Name: u.Name}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("%s/%s still exists after its finalizer ran (err=%v)", u.Namespace, u.Name, err)
	}
}

// TestReconcile_SecondUserCannotTakeAnOwnedUsername is the core of the
// ownership rule: two User objects in different namespaces resolve to one
// username. The second must not change the password, must not be able to
// delete the account, and must say why it is not Ready.
func TestReconcile_SecondUserCannotTakeAnOwnedUsername(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	first := namedUser("team-a", "ha", "shared")
	second := namedUser("team-b", "intruder", "shared")
	r, cli := newTestReconciler(t, pool, first, second)

	settle(t, r, "team-a", "ha")
	firstPassword := generatedPassword(t, cli, "team-a", "ha")
	assertVerifies(t, pool, "shared", firstPassword)
	assertOwner(t, pool, "shared", first)

	if _, err := reconcileUser(r, "team-b", "intruder"); err != nil {
		t.Fatalf("finalizer pass: %v", err)
	}
	res, err := reconcileUser(r, "team-b", "intruder")
	if err != nil {
		t.Fatalf("a conflict is a state to report, not an error to retry: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("a conflicting User must be rechecked later, in case the owner goes away")
	}

	cond := readyCondition(getUser(t, cli, "team-b", "intruder"))
	if cond.Status != metav1.ConditionFalse || cond.Reason != "UsernameConflict" {
		t.Errorf("Ready = %s/%s, want False/UsernameConflict", cond.Status, cond.Reason)
	}
	if !strings.Contains(cond.Message, `"shared"`) || !strings.Contains(cond.Message, "team-a/ha") {
		t.Errorf("message should name the username and its owner, got %q", cond.Message)
	}

	assertVerifies(t, pool, "shared", firstPassword)
	assertOwner(t, pool, "shared", first)

	var sec corev1.Secret
	err = cli.Get(context.Background(), types.NamespacedName{Namespace: "team-b", Name: "intruder-mqtt-credentials"}, &sec)
	if !apierrors.IsNotFound(err) {
		t.Errorf("a credentials Secret was generated for a User that does not hold the username (err=%v)", err)
	}

	deleteAndReconcile(t, r, cli, second)
	if n := countUsers(t, pool, "shared"); n != 1 {
		t.Fatalf("deleting the conflicting User removed the owner's account (%d rows)", n)
	}
	assertVerifies(t, pool, "shared", firstPassword)
}

// TestReconcile_UsernameIsTakenOverOnceTheOwnerIsDeleted follows a conflict
// through to its resolution.
func TestReconcile_UsernameIsTakenOverOnceTheOwnerIsDeleted(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	first := namedUser("team-a", "ha", "shared")
	second := namedUser("team-b", "next", "shared")
	r, cli := newTestReconciler(t, pool, first, second)

	settle(t, r, "team-a", "ha")
	settle(t, r, "team-b", "next")
	if cond := readyCondition(getUser(t, cli, "team-b", "next")); cond.Reason != "UsernameConflict" {
		t.Fatalf("setup: second User should be in conflict, got %s/%s", cond.Status, cond.Reason)
	}

	deleteAndReconcile(t, r, cli, first)
	if n := countUsers(t, pool, "shared"); n != 0 {
		t.Fatalf("owner's row survived its deletion (%d rows)", n)
	}

	if _, err := reconcileUser(r, "team-b", "next"); err != nil {
		t.Fatalf("reconcile after the owner left: %v", err)
	}
	if cond := readyCondition(getUser(t, cli, "team-b", "next")); cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %s/%s (%s), want True", cond.Status, cond.Reason, cond.Message)
	}
	assertOwner(t, pool, "shared", second)
	assertVerifies(t, pool, "shared", generatedPassword(t, cli, "team-b", "next"))
}

// TestReconcile_RowWhoseOwnerNoLongerExistsIsAdopted covers a row left
// behind by a User whose finalizer never ran (force-removed), in both
// shapes: nothing has that namespace/name any more, or a different object
// has since been created under it.
func TestReconcile_RowWhoseOwnerNoLongerExistsIsAdopted(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()
	stale, _ := bcrypt.GenerateFromPassword([]byte("stale"), bcrypt.MinCost)
	for _, row := range []struct{ username, ns, name, uid string }{
		{"orphan-a", "gone-ns", "gone", "uid-of-deleted-user"},
		{"orphan-b", "mqtt", "recreated", "uid-of-the-previous-incarnation"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO users(username, password_hash, owner_namespace, owner_name, owner_uid) VALUES($1,$2,$3,$4,$5)`,
			row.username, string(stale), row.ns, row.name, row.uid); err != nil {
			t.Fatalf("seed %s: %v", row.username, err)
		}
	}

	claimA := namedUser("mqtt", "claim-a", "orphan-a")
	// Same namespace/name as orphan-b's recorded owner, different UID, and
	// it does not itself want that username.
	recreated := namedUser("mqtt", "recreated", "something-else")
	claimB := namedUser("mqtt", "claim-b", "orphan-b")
	r, cli := newTestReconciler(t, pool, claimA, recreated, claimB)

	for _, u := range []*pgmqttv1alpha1.User{claimA, claimB} {
		settle(t, r, u.Namespace, u.Name)
		if cond := readyCondition(getUser(t, cli, u.Namespace, u.Name)); cond.Status != metav1.ConditionTrue {
			t.Errorf("%s: Ready = %s/%s (%s), want True", u.Name, cond.Status, cond.Reason, cond.Message)
		}
		assertOwner(t, pool, u.Spec.Username, u)
		assertVerifies(t, pool, u.Spec.Username, generatedPassword(t, cli, u.Namespace, u.Name))
	}
}

// TestReconcile_UnownedRowIsAdoptedByTheFirstUser covers rows from before
// the owner columns existed (and rows inserted by hand): the first User
// reconciled against one becomes its owner, and from then on it is
// protected like any other.
func TestReconcile_UnownedRowIsAdoptedByTheFirstUser(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	old, _ := bcrypt.GenerateFromPassword([]byte("pre-migration"), bcrypt.MinCost)
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO users(username, password_hash) VALUES('legacy', $1)`, string(old)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	first := namedUser("mqtt", "first", "legacy")
	second := namedUser("mqtt", "second", "legacy")
	r, cli := newTestReconciler(t, pool, first, second)

	settle(t, r, "mqtt", "first")
	assertOwner(t, pool, "legacy", first)
	assertVerifies(t, pool, "legacy", generatedPassword(t, cli, "mqtt", "first"))

	settle(t, r, "mqtt", "second")
	if cond := readyCondition(getUser(t, cli, "mqtt", "second")); cond.Reason != "UsernameConflict" {
		t.Errorf("second User: Ready = %s/%s, want False/UsernameConflict", cond.Status, cond.Reason)
	}
	assertOwner(t, pool, "legacy", first)
}

// TestReconcile_DeleteRemovesTheRowTheUserOwns: spec.username is edited
// and the User deleted before the edit is reconciled. Deleting by the
// spec-derived name left the User's real row behind with working
// credentials, and removed whichever account the new name pointed at.
func TestReconcile_DeleteRemovesTheRowTheUserOwns(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	renamed := namedUser("mqtt", "renamed", "alpha")
	bystander := namedUser("mqtt", "bystander", "beta")
	r, cli := newTestReconciler(t, pool, renamed, bystander)
	settle(t, r, "mqtt", "renamed")
	settle(t, r, "mqtt", "bystander")
	bystanderPassword := generatedPassword(t, cli, "mqtt", "bystander")

	live := getUser(t, cli, "mqtt", "renamed")
	live.Spec.Username = "beta"
	if err := cli.Update(context.Background(), &live); err != nil {
		t.Fatalf("edit spec.username: %v", err)
	}
	deleteAndReconcile(t, r, cli, &live)

	if n := countUsers(t, pool, "alpha"); n != 0 {
		t.Errorf("the deleted User's own row was left behind (%d rows): its credentials still work", n)
	}
	if n := countUsers(t, pool, "beta"); n != 1 {
		t.Fatalf("deleting the User removed another User's account (%d rows)", n)
	}
	assertVerifies(t, pool, "beta", bystanderPassword)
}

// TestReconcile_DeleteRemovesRowsFromBeforeOwnership covers Users last
// reconciled by a release that recorded no owner on the row.
func TestReconcile_DeleteRemovesRowsFromBeforeOwnership(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	ctx := context.Background()
	for _, name := range []string{"tracked", "untracked", "not-mine"} {
		if _, err := pool.Exec(ctx, `INSERT INTO users(username, password_hash) VALUES($1, 'h')`, name); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	if _, err := pool.Exec(ctx,
		`UPDATE users SET owner_namespace='x', owner_name='y', owner_uid='z' WHERE username='not-mine'`); err != nil {
		t.Fatalf("seed owner: %v", err)
	}

	// Status as written by a release with status.observedUsername; spec has
	// since been edited to something else.
	tracked := namedUser("mqtt", "u1", "edited-since")
	tracked.Finalizers = []string{"pgmqtt.io/user"}
	tracked.Status = pgmqttv1alpha1.UserStatus{
		ObservedUsername:     "tracked",
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "u1-mqtt-credentials"},
	}
	// Status from before observedUsername existed: only the spec-derived
	// name says which row this was.
	untracked := namedUser("mqtt", "u2", "untracked")
	untracked.Finalizers = []string{"pgmqtt.io/user"}
	untracked.Status = pgmqttv1alpha1.UserStatus{
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "u2-mqtt-credentials"},
	}
	// Same old-style status, but the row it points at belongs to another
	// User now.
	notMine := namedUser("mqtt", "u3", "not-mine")
	notMine.Finalizers = []string{"pgmqtt.io/user"}
	notMine.Status = pgmqttv1alpha1.UserStatus{
		CredentialsSecretRef: &corev1.LocalObjectReference{Name: "u3-mqtt-credentials"},
	}
	// Never reconciled at all: it has no claim on the row named after it.
	never := namedUser("mqtt", "u4", "tracked")
	never.Finalizers = []string{"pgmqtt.io/user"}

	r, cli := newTestReconciler(t, pool, tracked, untracked, notMine, never)

	deleteAndReconcile(t, r, cli, never)
	if n := countUsers(t, pool, "tracked"); n != 1 {
		t.Fatalf("a never-reconciled User deleted a row it has no record of owning (%d rows)", n)
	}
	deleteAndReconcile(t, r, cli, tracked)
	if n := countUsers(t, pool, "tracked"); n != 0 {
		t.Errorf("row named in status.observedUsername survived deletion (%d rows)", n)
	}
	deleteAndReconcile(t, r, cli, untracked)
	if n := countUsers(t, pool, "untracked"); n != 0 {
		t.Errorf("row of a User from before observedUsername survived deletion (%d rows)", n)
	}
	deleteAndReconcile(t, r, cli, notMine)
	if n := countUsers(t, pool, "not-mine"); n != 1 {
		t.Errorf("deleted a row owned by another User (%d rows)", n)
	}
}

// TestReconcile_RenameIntoATakenUsername: the User gives up its old name
// and does not get the new one.
func TestReconcile_RenameIntoATakenUsername(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	mover := namedUser("mqtt", "mover", "old-name")
	holder := namedUser("mqtt", "holder", "taken")
	r, cli := newTestReconciler(t, pool, mover, holder)
	settle(t, r, "mqtt", "mover")
	settle(t, r, "mqtt", "holder")
	holderPassword := generatedPassword(t, cli, "mqtt", "holder")

	live := getUser(t, cli, "mqtt", "mover")
	live.Spec.Username = "taken"
	if err := cli.Update(context.Background(), &live); err != nil {
		t.Fatalf("edit spec.username: %v", err)
	}
	if _, err := reconcileUser(r, "mqtt", "mover"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getUser(t, cli, "mqtt", "mover")
	if cond := readyCondition(got); cond.Status != metav1.ConditionFalse || cond.Reason != "UsernameConflict" {
		t.Errorf("Ready = %s/%s, want False/UsernameConflict", cond.Status, cond.Reason)
	}
	if got.Status.ObservedUsername != "" {
		t.Errorf("status.observedUsername = %q, but the User holds no row", got.Status.ObservedUsername)
	}
	if n := countUsers(t, pool, "old-name"); n != 0 {
		t.Errorf("the name the User gave up still authenticates (%d rows)", n)
	}
	assertOwner(t, pool, "taken", holder)
	assertVerifies(t, pool, "taken", holderPassword)
}

// TestReconcile_UsernameClaimedBetweenCheckAndWrite interleaves another
// owner's insert between this reconcile's ownership check and its write,
// which is what two managers do during a leader handoff. The write itself
// has to refuse.
func TestReconcile_UsernameClaimedBetweenCheckAndWrite(t *testing.T) {
	t.Parallel()
	pool := dbtest.FreshPool(t)
	late := namedUser("mqtt", "late", "contested")
	late.Finalizers = []string{"pgmqtt.io/user"}
	r, base := newTestReconciler(t, pool, late)

	winner, _ := bcrypt.GenerateFromPassword([]byte("winner"), bcrypt.MinCost)
	injected := false
	r.Client = interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		// The Secret is read after the ownership check and before the row
		// is written.
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok && !injected {
				injected = true
				if _, err := pool.Exec(ctx,
					`INSERT INTO users(username, password_hash, owner_namespace, owner_name, owner_uid)
					 VALUES('contested', $1, 'elsewhere', 'winner', 'uid-winner')`, string(winner)); err != nil {
					t.Errorf("inject competing row: %v", err)
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	res, err := reconcileUser(r, "mqtt", "late")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !injected {
		t.Fatal("test did not interleave: no Secret read happened")
	}
	if res.RequeueAfter <= 0 {
		t.Error("losing the race must requeue so the conflict gets reported")
	}
	assertVerifies(t, pool, "contested", []byte("winner"))
	if ns, name, _ := rowOwner(t, pool, "contested"); ns != "elsewhere" || name != "winner" {
		t.Errorf("row owner overwritten: now %s/%s", ns, name)
	}
	if cond := readyCondition(getUser(t, base, "mqtt", "late")); cond.Status == metav1.ConditionTrue {
		t.Error("User reports Ready=True for a username it does not hold")
	}
}
