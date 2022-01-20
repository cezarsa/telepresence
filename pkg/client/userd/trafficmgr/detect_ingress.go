package trafficmgr

import (
	"context"

	core "k8s.io/api/core/v1"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

func (tm *TrafficManager) IngressInfos(c context.Context) ([]*manager.IngressInfo, error) {
	tm.insLock.Lock()
	defer tm.insLock.Unlock()

	ingressInfo := tm.ingressInfo
	if ingressInfo == nil {
		tm.insLock.Unlock()
		ingressInfo := tm.detectIngressBehavior(c)
		tm.insLock.Lock()
		tm.ingressInfo = ingressInfo
	}
	is := make([]*manager.IngressInfo, len(tm.ingressInfo))
	copy(is, tm.ingressInfo)
	return is, nil
}

func (tm *TrafficManager) detectIngressBehavior(c context.Context) []*manager.IngressInfo {
	loadBalancers := tm.findAllSvcByType(c, core.ServiceTypeLoadBalancer)
	type portFilter func(p *core.ServicePort) bool
	findTCPPort := func(ports []core.ServicePort, filter portFilter) *core.ServicePort {
		for i := range ports {
			p := &ports[i]
			if p.Protocol == "" || p.Protocol == "TCP" && filter(p) {
				return p
			}
		}
		return nil
	}

	// filters in priority order.
	portFilters := []portFilter{
		func(p *core.ServicePort) bool { return p.Name == "https" },
		func(p *core.ServicePort) bool { return p.Port == 443 },
		func(p *core.ServicePort) bool { return p.Name == "http" },
		func(p *core.ServicePort) bool { return p.Port == 80 },
		func(p *core.ServicePort) bool { return true },
	}

	iis := make([]*manager.IngressInfo, 0, len(loadBalancers))
	for _, lb := range loadBalancers {
		spec := &lb.Spec
		var port *core.ServicePort
		for _, pf := range portFilters {
			if p := findTCPPort(spec.Ports, pf); p != nil {
				port = p
				break
			}
		}
		if port == nil {
			continue
		}

		iis = append(iis, &manager.IngressInfo{
			Host:   lb.Name + "." + lb.Namespace,
			UseTls: port.Port == 443,
			Port:   port.Port,
		})
	}
	return iis
}

// findAllSvcByType finds services with the given service type in all namespaces of the cluster returns
// a slice containing a copy of those services.
func (tm *TrafficManager) findAllSvcByType(c context.Context, svcType core.ServiceType) []*core.Service {
	var services []*core.Service
	checked := map[string]struct{}{}

	findLB := func(ns string) {
		tm.wlWatcher.eachService(c, ns, func(wi *core.Service) {
			if wi.Spec.Type == svcType {
				services = append(services, wi)
			}
		})
	}

	// Favor namespaces where the user has access
	for _, ns := range tm.GetCurrentNamespaces(true) {
		checked[ns] = struct{}{}
		findLB(ns)
	}
	if len(services) == 0 {
		// No ingress found in accessible namespaces. Let's try the ones where user don't have access
		// because user might not have access to the namespace where the load balancer resides
		for _, ns := range tm.GetCurrentNamespaces(false) {
			if _, ok := checked[ns]; !ok {
				findLB(ns)
			}
		}
	}
	return services
}
