package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Overview struct {
	NodeCount           int
	NamespaceCount      int
	ContainerCount      int
	UnhealthyCount      int
	TotalNodeCPU        float64
	TotalNodeMemoryGB   float64
	TotalContainerCPU   float64
	TotalContainerMemGB float64
	TrafficEdges        int
	EventCount          int
}

type Node struct {
	Name         string
	Status       string
	CPUPercent   float64
	MemoryGB     float64
	MemoryUsedGB float64
	Uptime       string
	Pods         int
	Runtime      string
	Kernel       string
	Zone         string
	Pressure     string
}

type Namespace struct {
	Name               string
	Containers         int
	Services           int
	CPUPercent         float64
	CPUCores           float64
	CPUAllocatedCores  float64
	CPUAllocPercent    float64
	MemoryUsedGB       float64
	MemoryAllocatedGB  float64
	MemoryAllocPercent float64
	Restarts24h        int
	Warnings           int
	InboundRPS         int
	OutboundRPS        int
	TopService         string
	TopServiceLatency  string
}

type Container struct {
	Name               string
	PodName            string
	ContainerName      string
	Namespace          string
	Node               string
	Status             string
	CPUPercent         float64
	CPUCores           float64
	CPUAllocatedCores  float64
	CPUAllocPercent    float64
	MemoryMB           int
	MemoryAllocatedMB  int
	MemoryAllocPercent float64
	Restarts24h        int
	RxKB               int
	TxKB               int
	LastEvent          string
	LastLogLine        string
	Owner              string
	Age                string
}

type TrafficEdge struct {
	From     string
	To       string
	Protocol string
	Details  string
	Status   string
}

type Event struct {
	Time     string
	Scope    string
	Kind     string
	Message  string
	Severity string
}

type Alert struct {
	Name   string
	Scope  string
	Reason string
	Status string
	Since  string
}

type DashboardData struct {
	GeneratedAt string
	Overview    Overview
	Nodes       []Node
	Namespaces  []Namespace
	Containers  []Container
	Traffic     []TrafficEdge
	Events      []Event
	Alerts      []Alert
	TopLogs     []Container
	Mode        string
	Live        bool
	TrafficNote string
	SourceNote  string
}

type kubeClient struct {
	baseURL      string
	httpClient   *http.Client
	streamClient *http.Client
	token        string
}

type objectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	Labels            map[string]string `json:"labels"`
	OwnerReferences   []ownerReference  `json:"ownerReferences"`
	UID               string            `json:"uid"`
}

type ownerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type nodeList struct {
	Items []nodeItem `json:"items"`
}

type nodeItem struct {
	Metadata objectMeta `json:"metadata"`
	Status   struct {
		Capacity   map[string]string `json:"capacity"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
		NodeInfo struct {
			ContainerRuntimeVersion string `json:"containerRuntimeVersion"`
			KernelVersion           string `json:"kernelVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

type podList struct {
	Items []podItem `json:"items"`
}

type podItem struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		NodeName   string `json:"nodeName"`
		Containers []struct {
			Name      string `json:"name"`
			Resources struct {
				Requests map[string]string `json:"requests"`
				Limits   map[string]string `json:"limits"`
			} `json:"resources"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			Name         string `json:"name"`
			RestartCount int    `json:"restartCount"`
			Ready        bool   `json:"ready"`
			State        struct {
				Waiting *struct {
					Reason string `json:"reason"`
				} `json:"waiting"`
				Terminated *struct {
					Reason string `json:"reason"`
				} `json:"terminated"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

type serviceList struct {
	Items []serviceItem `json:"items"`
}

type serviceItem struct {
	Metadata objectMeta `json:"metadata"`
	Spec     struct {
		Selector map[string]string `json:"selector"`
		Ports    []struct {
			Protocol string `json:"protocol"`
			Port     int    `json:"port"`
		} `json:"ports"`
	} `json:"spec"`
}

type eventList struct {
	Items []eventItem `json:"items"`
}

type eventItem struct {
	Metadata       objectMeta `json:"metadata"`
	Type           string     `json:"type"`
	Reason         string     `json:"reason"`
	Message        string     `json:"message"`
	EventTime      time.Time  `json:"eventTime"`
	LastTimestamp  time.Time  `json:"lastTimestamp"`
	InvolvedObject struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"involvedObject"`
}

type metricsNodeList struct {
	Items []struct {
		Metadata objectMeta        `json:"metadata"`
		Usage    map[string]string `json:"usage"`
	} `json:"items"`
}

type metricsPodList struct {
	Items []struct {
		Metadata   objectMeta `json:"metadata"`
		Containers []struct {
			Name  string            `json:"name"`
			Usage map[string]string `json:"usage"`
		} `json:"containers"`
	} `json:"items"`
}

type namespaceAgg struct {
	Name              string
	Containers        int
	Services          int
	CPUPercent        float64
	CPUCores          float64
	CPUAllocatedCores float64
	MemoryGB          float64
	MemoryAllocatedGB float64
	Restarts          int
	Warnings          int
	TopService        string
	TopServiceMS      string
	SortCPU           float64
}

func main() {
	funcs := template.FuncMap{
		"formatPercent": func(v float64) string { return fmt.Sprintf("%.1f%%", v) },
		"formatGB":      func(v float64) string { return fmt.Sprintf("%.1f GB", v) },
		"formatMB":      func(v int) string { return fmt.Sprintf("%d MB", v) },
		"formatCPU":     formatCPU,
	}

	tmpl := template.Must(template.New("index").Funcs(funcs).Parse(indexTemplate))
	addr := envOr("WAISERVABILITY_LISTEN", ":7070")
	client, live := newKubeClient()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		data := snapshot(r.Context(), client, live)
		_ = json.NewEncoder(w).Encode(data)
	})

	http.HandleFunc("/api/logs", func(w http.ResponseWriter, r *http.Request) {
		if !live || client == nil {
			http.Error(w, "live logs unavailable", http.StatusServiceUnavailable)
			return
		}
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		pod := strings.TrimSpace(r.URL.Query().Get("pod"))
		container := strings.TrimSpace(r.URL.Query().Get("container"))
		if namespace == "" || pod == "" || container == "" {
			http.Error(w, "namespace, pod, and container are required", http.StatusBadRequest)
			return
		}
		logs, err := client.podLogs(r.Context(), namespace, pod, container, 120)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"logs": logs})
	})

	http.HandleFunc("/api/logs/stream", func(w http.ResponseWriter, r *http.Request) {
		if !live || client == nil {
			http.Error(w, "live logs unavailable", http.StatusServiceUnavailable)
			return
		}
		namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
		pod := strings.TrimSpace(r.URL.Query().Get("pod"))
		container := strings.TrimSpace(r.URL.Query().Get("container"))
		if namespace == "" || pod == "" || container == "" {
			http.Error(w, "namespace, pod, and container are required", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		if err := client.streamPodLogs(r.Context(), namespace, pod, container, 120, w, flusher); err != nil {
			log.Printf("waiservability: stream logs ended: %v", err)
		}
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data := snapshot(r.Context(), client, live)
		data.Mode = strings.TrimSpace(r.URL.Query().Get("mode"))
		if data.Mode == "" {
			data.Mode = "light"
		}
		if err := tmpl.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	log.Printf("waiservability listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func newKubeClient() (*kubeClient, bool) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, false
	}

	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Printf("waiservability: no service account token: %v", err)
		return nil, false
	}

	caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		log.Printf("waiservability: no cluster ca: %v", err)
		return nil, false
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		log.Printf("waiservability: failed to load cluster ca")
		return nil, false
	}

	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	return &kubeClient{
		baseURL: fmt.Sprintf("https://%s:%s", host, port),
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		streamClient: &http.Client{
			Transport: transport,
		},
		token: strings.TrimSpace(string(tokenBytes)),
	}, true
}

func snapshot(ctx context.Context, client *kubeClient, live bool) DashboardData {
	if !live || client == nil {
		return demoSnapshot()
	}
	data, err := liveSnapshot(ctx, client)
	if err != nil {
		log.Printf("waiservability: live snapshot failed: %v", err)
		fallback := demoSnapshot()
		fallback.SourceNote = "live cluster read failed. showing demo data."
		return fallback
	}
	return data
}

func liveSnapshot(ctx context.Context, client *kubeClient) (DashboardData, error) {
	var nodes nodeList
	if err := client.get(ctx, "/api/v1/nodes", &nodes); err != nil {
		return DashboardData{}, err
	}

	var pods podList
	if err := client.get(ctx, "/api/v1/pods", &pods); err != nil {
		return DashboardData{}, err
	}

	var services serviceList
	if err := client.get(ctx, "/api/v1/services", &services); err != nil {
		return DashboardData{}, err
	}

	var events eventList
	if err := client.get(ctx, "/api/v1/events", &events); err != nil {
		return DashboardData{}, err
	}

	var nodeMetrics metricsNodeList
	_ = client.get(ctx, "/apis/metrics.k8s.io/v1beta1/nodes", &nodeMetrics)

	var podMetrics metricsPodList
	_ = client.get(ctx, "/apis/metrics.k8s.io/v1beta1/pods", &podMetrics)

	nodeMetricMap := map[string]map[string]string{}
	for _, item := range nodeMetrics.Items {
		nodeMetricMap[item.Metadata.Name] = item.Usage
	}

	type podContainerMetric struct{ cpu, memory string }
	podMetricMap := map[string]map[string]podContainerMetric{}
	for _, item := range podMetrics.Items {
		key := item.Metadata.Namespace + "/" + item.Metadata.Name
		podMetricMap[key] = map[string]podContainerMetric{}
		for _, c := range item.Containers {
			podMetricMap[key][c.Name] = podContainerMetric{cpu: c.Usage["cpu"], memory: c.Usage["memory"]}
		}
	}

	podsByNode := map[string]int{}
	namespaceMap := map[string]*namespaceAgg{}
	serviceCounts := map[string]int{}
	lastEventByPod := map[string]string{}
	eventWarningsByNamespace := map[string]int{}
	alerts := []Alert{}

	for _, service := range services.Items {
		serviceCounts[service.Metadata.Namespace]++
	}

	for _, event := range events.Items {
		ts := eventTime(event)
		scope := event.InvolvedObject.Name
		if scope == "" {
			scope = event.Metadata.Name
		}
		if event.InvolvedObject.Kind == "Pod" {
			lastEventByPod[event.InvolvedObject.Namespace+"/"+event.InvolvedObject.Name] = event.Message
		}
		if strings.EqualFold(event.Type, "Warning") {
			eventWarningsByNamespace[event.Metadata.Namespace]++
			alerts = append(alerts, Alert{Name: event.Reason, Scope: scope, Reason: event.Message, Status: "open", Since: ago(ts)})
		}
	}

	nodeCapCPU := map[string]float64{}
	nodeCapMem := map[string]float64{}
	nodesOut := make([]Node, 0, len(nodes.Items))
	overview := Overview{}
	totalClusterCPUCapacity := 0.0
	totalClusterMemoryCapacityGB := 0.0

	for _, item := range nodes.Items {
		cpuCap := parseCPU(item.Status.Capacity["cpu"])
		memCapGB := bytesToGB(parseBytes(item.Status.Capacity["memory"]))
		totalClusterCPUCapacity += cpuCap
		totalClusterMemoryCapacityGB += memCapGB
		nodeCapCPU[item.Metadata.Name] = cpuCap
		nodeCapMem[item.Metadata.Name] = memCapGB

		usage := nodeMetricMap[item.Metadata.Name]
		cpuPercent := 0.0
		if cpuCap > 0 {
			cpuPercent = (parseCPU(usage["cpu"]) / cpuCap) * 100
		}
		memUsedGB := bytesToGB(parseBytes(usage["memory"]))
		memPercent := 0.0
		if memCapGB > 0 {
			memPercent = (memUsedGB / memCapGB) * 100
		}

		pressure := "low"
		status := "healthy"
		for _, cond := range item.Status.Conditions {
			switch cond.Type {
			case "Ready":
				if cond.Status != "True" {
					status = "warning"
				}
			case "MemoryPressure", "DiskPressure", "PIDPressure":
				if cond.Status == "True" {
					pressure = "high"
					status = "warning"
				}
			}
		}
		if pressure == "low" && memPercent >= 80 {
			pressure = "high"
			status = "warning"
		} else if pressure == "low" && memPercent >= 60 {
			pressure = "medium"
		}

		runtime := item.Status.NodeInfo.ContainerRuntimeVersion
		if idx := strings.Index(runtime, "://"); idx >= 0 {
			runtime = runtime[:idx]
		}

		nodesOut = append(nodesOut, Node{
			Name:         item.Metadata.Name,
			Status:       status,
			CPUPercent:   clamp(cpuPercent),
			MemoryGB:     memCapGB,
			MemoryUsedGB: memUsedGB,
			Uptime:       age(item.Metadata.CreationTimestamp),
			Pods:         0,
			Runtime:      runtime,
			Kernel:       item.Status.NodeInfo.KernelVersion,
			Zone:         firstNonEmpty(item.Metadata.Labels["topology.kubernetes.io/zone"], "local"),
			Pressure:     pressure,
		})

		overview.TotalNodeCPU += clamp(cpuPercent)
		overview.TotalNodeMemoryGB += memUsedGB
		if status != "healthy" {
			overview.UnhealthyCount++
		}
	}

	containersOut := []Container{}
	for _, pod := range pods.Items {
		if pod.Metadata.Namespace == "kube-system" && strings.Contains(pod.Metadata.Name, "metrics-server") {
			// keep it, no special skip
		}
		podsByNode[pod.Spec.NodeName]++
		ns := ensureNamespace(namespaceMap, pod.Metadata.Namespace)
		ns.Services = serviceCounts[pod.Metadata.Namespace]

		podKey := pod.Metadata.Namespace + "/" + pod.Metadata.Name
		containerMetrics := podMetricMap[podKey]
		containerSpecMap := map[string]struct {
			cpuRequest   float64
			memRequestMB int
		}{}
		for _, specContainer := range pod.Spec.Containers {
			containerSpecMap[specContainer.Name] = struct {
				cpuRequest   float64
				memRequestMB int
			}{
				cpuRequest:   parseCPU(specContainer.Resources.Requests["cpu"]),
				memRequestMB: int(math.Round(parseBytes(specContainer.Resources.Requests["memory"]) / (1024 * 1024))),
			}
		}
		podCPUPercent := 0.0
		podCPUCores := 0.0
		podCPUAllocatedCores := 0.0
		podMemoryMB := 0
		podMemoryAllocatedMB := 0
		restarts := 0
		status := "healthy"
		for _, cs := range pod.Status.ContainerStatuses {
			restarts += cs.RestartCount
			metric := containerMetrics[cs.Name]
			specResources := containerSpecMap[cs.Name]
			cpuCores := parseCPU(metric.cpu)
			cpuPercent := 0.0
			if capCPU := nodeCapCPU[pod.Spec.NodeName]; capCPU > 0 {
				cpuPercent = (cpuCores / capCPU) * 100
			}
			cpuAllocPercent := 0.0
			if specResources.cpuRequest > 0 {
				cpuAllocPercent = (cpuCores / specResources.cpuRequest) * 100
			}
			memoryMB := int(math.Round(parseBytes(metric.memory) / (1024 * 1024)))
			memoryAllocPercent := 0.0
			if specResources.memRequestMB > 0 {
				memoryAllocPercent = (float64(memoryMB) / float64(specResources.memRequestMB)) * 100
			}
			podCPUPercent += cpuPercent
			podCPUCores += cpuCores
			podCPUAllocatedCores += specResources.cpuRequest
			podMemoryMB += memoryMB
			podMemoryAllocatedMB += specResources.memRequestMB

			containerStatus := "healthy"
			if !cs.Ready || pod.Status.Phase != "Running" {
				containerStatus = "warning"
			}
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				containerStatus = "warning"
			}
			if cs.State.Terminated != nil && cs.State.Terminated.Reason != "Completed" {
				containerStatus = "warning"
			}
			if containerStatus != "healthy" {
				status = "warning"
			}

			containersOut = append(containersOut, Container{
				Name:               cs.Name,
				PodName:            pod.Metadata.Name,
				ContainerName:      cs.Name,
				Namespace:          pod.Metadata.Namespace,
				Node:               pod.Spec.NodeName,
				Status:             containerStatus,
				CPUPercent:         clamp(cpuPercent),
				CPUCores:           cpuCores,
				CPUAllocatedCores:  specResources.cpuRequest,
				CPUAllocPercent:    clamp(cpuAllocPercent),
				MemoryMB:           memoryMB,
				MemoryAllocatedMB:  specResources.memRequestMB,
				MemoryAllocPercent: clamp(memoryAllocPercent),
				Restarts24h:        cs.RestartCount,
				LastEvent:          firstNonEmpty(lastEventByPod[podKey], "no recent event"),
				Owner:              ownerLabel(pod.Metadata.OwnerReferences),
				Age:                age(pod.Metadata.CreationTimestamp),
			})
		}

		ns.Containers += len(pod.Spec.Containers)
		ns.CPUPercent += podCPUPercent
		ns.CPUCores += podCPUCores
		ns.CPUAllocatedCores += podCPUAllocatedCores
		ns.MemoryGB += float64(podMemoryMB) / 1024
		ns.MemoryAllocatedGB += float64(podMemoryAllocatedMB) / 1024
		ns.Restarts += restarts
		ns.Warnings += boolInt(status != "healthy") + eventWarningsByNamespace[pod.Metadata.Namespace]
		if podCPUPercent > ns.SortCPU {
			ns.SortCPU = podCPUPercent
			ns.TopService = pod.Metadata.Name
		}

		overview.ContainerCount += len(pod.Spec.Containers)
		overview.TotalContainerCPU += podCPUPercent
		overview.TotalContainerMemGB += float64(podMemoryMB) / 1024
		if status != "healthy" || restarts > 0 {
			overview.UnhealthyCount++
		}
		if restarts > 0 {
			alerts = append(alerts, Alert{Name: "restart", Scope: pod.Metadata.Name, Reason: fmt.Sprintf("%d restarts", restarts), Status: "watch", Since: age(pod.Metadata.CreationTimestamp)})
		}
	}

	for i := range nodesOut {
		nodesOut[i].Pods = podsByNode[nodesOut[i].Name]
	}

	namespacesOut := make([]Namespace, 0, len(namespaceMap))
	for _, ns := range namespaceMap {
		cpuAllocPercent := 0.0
		if totalClusterCPUCapacity > 0 {
			cpuAllocPercent = (ns.CPUCores / totalClusterCPUCapacity) * 100
		}
		memoryAllocPercent := 0.0
		if totalClusterMemoryCapacityGB > 0 {
			memoryAllocPercent = (ns.MemoryGB / totalClusterMemoryCapacityGB) * 100
		}
		namespacesOut = append(namespacesOut, Namespace{
			Name:               ns.Name,
			Containers:         ns.Containers,
			Services:           ns.Services,
			CPUPercent:         clamp(ns.CPUPercent),
			CPUCores:           ns.CPUCores,
			CPUAllocatedCores:  ns.CPUAllocatedCores,
			CPUAllocPercent:    clamp(cpuAllocPercent),
			MemoryUsedGB:       ns.MemoryGB,
			MemoryAllocatedGB:  ns.MemoryAllocatedGB,
			MemoryAllocPercent: clamp(memoryAllocPercent),
			Restarts24h:        ns.Restarts,
			Warnings:           ns.Warnings,
			TopService:         firstNonEmpty(ns.TopService, "-"),
			TopServiceLatency:  "-",
		})
	}
	sort.Slice(namespacesOut, func(i, j int) bool { return namespacesOut[i].CPUPercent > namespacesOut[j].CPUPercent })

	traffic := buildTrafficEdges(services.Items, pods.Items)
	sort.Slice(traffic, func(i, j int) bool {
		if traffic[i].From == traffic[j].From {
			return traffic[i].To < traffic[j].To
		}
		return traffic[i].From < traffic[j].From
	})

	eventsOut := make([]Event, 0, len(events.Items))
	sort.Slice(events.Items, func(i, j int) bool { return eventTime(events.Items[i]).After(eventTime(events.Items[j])) })
	for _, event := range events.Items {
		if len(eventsOut) >= 200 {
			break
		}
		scope := event.InvolvedObject.Name
		if event.InvolvedObject.Namespace != "" {
			scope = event.InvolvedObject.Namespace + "/" + scope
		}
		eventsOut = append(eventsOut, Event{
			Time:     eventTime(event).Format("15:04"),
			Scope:    scope,
			Kind:     strings.ToLower(firstNonEmpty(event.Reason, event.Type)),
			Message:  event.Message,
			Severity: strings.ToLower(firstNonEmpty(event.Type, "normal")),
		})
	}

	sort.Slice(containersOut, func(i, j int) bool { return containersOut[i].CPUPercent > containersOut[j].CPUPercent })
	topLogs := append([]Container(nil), containersOut...)
	if len(topLogs) > 5 {
		topLogs = topLogs[:5]
	}
	for i := range topLogs {
		line, err := client.podContainerLogLine(ctx, topLogs[i].Namespace, topLogs[i].PodName, topLogs[i].ContainerName)
		if err == nil && strings.TrimSpace(line) != "" {
			topLogs[i].LastLogLine = line
		} else {
			topLogs[i].LastLogLine = "log unavailable"
		}
	}

	seenAlerts := map[string]bool{}
	filteredAlerts := []Alert{}
	for _, alert := range alerts {
		key := alert.Name + "|" + alert.Scope + "|" + alert.Reason
		if seenAlerts[key] {
			continue
		}
		seenAlerts[key] = true
		filteredAlerts = append(filteredAlerts, alert)
		if len(filteredAlerts) >= 12 {
			break
		}
	}

	overview.NodeCount = len(nodesOut)
	overview.NamespaceCount = len(namespacesOut)
	overview.TrafficEdges = len(traffic)
	overview.EventCount = len(eventsOut)

	return DashboardData{
		GeneratedAt: time.Now().Format("2006-01-02 15:04:05"),
		Overview:    overview,
		Nodes:       nodesOut,
		Namespaces:  namespacesOut,
		Containers:  containersOut,
		Traffic:     traffic,
		Events:      eventsOut,
		Alerts:      filteredAlerts,
		TopLogs:     topLogs,
		Live:        true,
		SourceNote:  "live node, pod, event, service, and log data from kubernetes",
		TrafficNote: "traffic view currently shows real service-to-pod paths. live request volume needs a network telemetry source like cilium/hubble or a service mesh.",
	}, nil
}

func (c *kubeClient) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *kubeClient) podContainerLogLine(ctx context.Context, namespace, pod, container string) (string, error) {
	logs, err := c.podLogs(ctx, namespace, pod, container, 1)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(logs), nil
}

func (c *kubeClient) podLogs(ctx context.Context, namespace, pod, container string, tailLines int) (string, error) {
	q := url.Values{}
	q.Set("tailLines", strconv.Itoa(tailLines))
	q.Set("timestamps", "false")
	if container != "" {
		q.Set("container", container)
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?%s", namespace, pod, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("log fetch failed: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func (c *kubeClient) streamPodLogs(ctx context.Context, namespace, pod, container string, tailLines int, w io.Writer, flusher http.Flusher) error {
	q := url.Values{}
	q.Set("follow", "true")
	q.Set("tailLines", strconv.Itoa(tailLines))
	q.Set("timestamps", "false")
	if container != "" {
		q.Set("container", container)
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?%s", namespace, pod, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("log stream failed: %s", resp.Status)
	}

	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			payload := strings.TrimRight(line, "\n")
			// SSE format: prefix every line with "data: "
			if _, werr := fmt.Fprintf(w, "data: %s\n\n", payload); werr != nil {
				return werr
			}
			flusher.Flush()
		}
		if err != nil {
			return err
		}
	}
}

func buildTrafficEdges(services []serviceItem, pods []podItem) []TrafficEdge {
	edges := []TrafficEdge{}
	for _, service := range services {
		if len(service.Spec.Selector) == 0 {
			continue
		}
		protocol := "tcp"
		if len(service.Spec.Ports) > 0 && service.Spec.Ports[0].Protocol != "" {
			protocol = strings.ToLower(service.Spec.Ports[0].Protocol)
		}
		for _, pod := range pods {
			if pod.Metadata.Namespace != service.Metadata.Namespace {
				continue
			}
			if !labelsMatch(service.Spec.Selector, pod.Metadata.Labels) {
				continue
			}
			edges = append(edges, TrafficEdge{
				From:     service.Metadata.Namespace + "/" + service.Metadata.Name,
				To:       pod.Metadata.Namespace + "/" + pod.Metadata.Name,
				Protocol: protocol,
				Details:  fmt.Sprintf("service routes to %d containers on %s", len(pod.Spec.Containers), firstNonEmpty(pod.Spec.NodeName, "unknown node")),
				Status:   strings.ToLower(firstNonEmpty(pod.Status.Phase, "unknown")),
			})
		}
	}
	return edges
}

func labelsMatch(selector, labels map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func ensureNamespace(m map[string]*namespaceAgg, name string) *namespaceAgg {
	if m[name] == nil {
		m[name] = &namespaceAgg{Name: name}
	}
	return m[name]
}

func parseCPU(value string) float64 {
	if value == "" {
		return 0
	}
	if strings.HasSuffix(value, "n") {
		f, _ := strconv.ParseFloat(strings.TrimSuffix(value, "n"), 64)
		return f / 1000000000
	}
	if strings.HasSuffix(value, "u") {
		f, _ := strconv.ParseFloat(strings.TrimSuffix(value, "u"), 64)
		return f / 1000000
	}
	if strings.HasSuffix(value, "m") {
		f, _ := strconv.ParseFloat(strings.TrimSuffix(value, "m"), 64)
		return f / 1000
	}
	f, _ := strconv.ParseFloat(value, 64)
	return f
}

func parseBytes(value string) float64 {
	if value == "" {
		return 0
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"Ki", 1024}, {"Mi", 1024 * 1024}, {"Gi", 1024 * 1024 * 1024},
		{"Ti", 1024 * 1024 * 1024 * 1024}, {"K", 1000}, {"M", 1000 * 1000}, {"G", 1000 * 1000 * 1000},
	}
	for _, unit := range units {
		if strings.HasSuffix(value, unit.suffix) {
			f, _ := strconv.ParseFloat(strings.TrimSuffix(value, unit.suffix), 64)
			return f * unit.mult
		}
	}
	f, _ := strconv.ParseFloat(value, 64)
	return f
}

func bytesToGB(v float64) float64 { return v / (1024 * 1024 * 1024) }

func age(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := time.Since(t)
	if d < 0 {
		d = 0
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	if days > 0 {
		return fmt.Sprintf("%dd %dh", days, hours)
	}
	mins := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", int(d.Hours()), mins)
}

func ago(t time.Time) string {
	if t.IsZero() {
		return "now"
	}
	d := time.Since(t)
	if d < time.Minute {
		return "now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func ownerLabel(refs []ownerReference) string {
	if len(refs) == 0 {
		return "pod"
	}
	return strings.ToLower(refs[0].Kind) + "/" + refs[0].Name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func eventTime(event eventItem) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp
	}
	return event.Metadata.CreationTimestamp
}

func clamp(v float64) float64 {
	if v < 0 {
		return 0
	}
	return math.Round(v*10) / 10
}

func formatCPU(v float64) string {
	if v >= 1 {
		return fmt.Sprintf("%.2f cores", v)
	}
	mCPU := v * 1000
	if mCPU >= 10 {
		return fmt.Sprintf("%.0f mCPU", mCPU)
	}
	if mCPU > 0 {
		return fmt.Sprintf("%.1f mCPU", mCPU)
	}
	return "0 mCPU"
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func demoSnapshot() DashboardData {
	return DashboardData{
		GeneratedAt: time.Now().Format("2006-01-02 15:04:05"),
		Overview:    Overview{NodeCount: 0, NamespaceCount: 0, ContainerCount: 0, TrafficEdges: 0},
		Live:        false,
		SourceNote:  "no kubernetes api found. deploy in cluster to see live data.",
		TrafficNote: "traffic view needs kubernetes service data and optional network telemetry.",
	}
}
