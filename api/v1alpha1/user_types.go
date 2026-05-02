package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UserSpec defines the desired state of User.
type UserSpec struct {
	// Username is the MQTT username. Defaults to metadata.name when empty.
	// +optional
	Username string `json:"username,omitempty"`

	// PasswordSecretRef references an existing Secret holding the password.
	// When omitted, the reconciler generates a Secret with the wire details
	// a client needs (cnpg-style: username, password, host, port, uri, ...).
	// +optional
	PasswordSecretRef *corev1.SecretKeySelector `json:"passwordSecretRef,omitempty"`
}

// UserStatus defines the observed state of User.
type UserStatus struct {
	// Conditions hold the latest reconciliation conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// CredentialsSecretRef is the Secret the reconciler is reading from
	// (and which it auto-generated when no PasswordSecretRef was set).
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`

	// ObservedSecretHash is sha256 of the password value last reconciled
	// successfully into the database. Used to skip re-hashing on no-op.
	// +optional
	ObservedSecretHash string `json:"observedSecretHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mqu
// +kubebuilder:printcolumn:name="Username",type=string,JSONPath=`.spec.username`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// User is the Schema for the users API.
type User struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UserSpec   `json:"spec,omitempty"`
	Status UserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// UserList contains a list of User.
type UserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []User `json:"items"`
}

func init() {
	SchemeBuilder.Register(&User{}, &UserList{})
}
