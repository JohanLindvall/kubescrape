// Package services maintains an in-memory index of Services so pods can be
// matched against the Services that select them (for service-annotation
// based scrape discovery).
package services

import (
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Service is the subset of a Kubernetes Service needed for scrape discovery.
// Instances are immutable once published; do not mutate them.
type Service struct {
	Name        string
	Namespace   string
	UID         string
	Labels      map[string]string
	Annotations map[string]string
	Selector    map[string]string
	Ports       []Port
}

// Port is one service port with its target-port mapping.
type Port struct {
	Name string
	Port int32
	// TargetPortName is set when targetPort references a named container
	// port; otherwise TargetPortNum holds the numeric target port (0 means
	// unset, in which case the target port equals Port).
	TargetPortName string
	TargetPortNum  int32
}

// Index is safe for concurrent use.
type Index struct {
	mu          sync.RWMutex
	byNamespace map[string]map[types.UID]*Service
}

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{byNamespace: make(map[string]map[types.UID]*Service)}
}

// Upsert records the current state of a service.
func (ix *Index) Upsert(svc *corev1.Service) {
	rec := &Service{
		Name:        svc.Name,
		Namespace:   svc.Namespace,
		UID:         string(svc.UID),
		Labels:      copyMap(svc.Labels),
		Annotations: copyMap(svc.Annotations),
		Selector:    copyMap(svc.Spec.Selector),
	}
	for _, p := range svc.Spec.Ports {
		port := Port{Name: p.Name, Port: p.Port}
		switch p.TargetPort.Type {
		case intstr.String:
			port.TargetPortName = p.TargetPort.StrVal
		case intstr.Int:
			port.TargetPortNum = p.TargetPort.IntVal
		}
		rec.Ports = append(rec.Ports, port)
	}

	ix.mu.Lock()
	defer ix.mu.Unlock()
	m := ix.byNamespace[svc.Namespace]
	if m == nil {
		m = make(map[types.UID]*Service)
		ix.byNamespace[svc.Namespace] = m
	}
	m[svc.UID] = rec
}

// Delete removes a service.
func (ix *Index) Delete(namespace string, uid types.UID) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	m := ix.byNamespace[namespace]
	if m == nil {
		return
	}
	delete(m, uid)
	if len(m) == 0 {
		delete(ix.byNamespace, namespace)
	}
}

// All returns the services in the given namespaces (nil = every namespace).
func (ix *Index) All(namespaces []string) []*Service {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	var out []*Service
	appendNS := func(ns string) {
		for _, svc := range ix.byNamespace[ns] {
			out = append(out, svc)
		}
	}
	if namespaces == nil {
		for ns := range ix.byNamespace {
			appendNS(ns)
		}
		return out
	}
	for _, ns := range namespaces {
		appendNS(ns)
	}
	return out
}

// Matching returns the services in namespace whose selector matches the
// given pod labels. Services without a selector never match.
func (ix *Index) Matching(namespace string, podLabels map[string]string) []*Service {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	var out []*Service
	for _, svc := range ix.byNamespace[namespace] {
		if len(svc.Selector) == 0 {
			continue
		}
		if selects(svc.Selector, podLabels) {
			out = append(out, svc)
		}
	}
	return out
}

func selects(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
