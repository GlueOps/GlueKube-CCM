
from typing import Union

from fastapi import FastAPI
from kubernetes import client, config
from kubernetes.client.rest import ApiException

app = FastAPI()


group = "externaldns.k8s.io"
version = "v1alpha1"
plural = "dnsendpoints"
namespace = "default"



def apply_custom_object(group, version, namespace, plural, body):
    custom_api = client.CustomObjectsApi()
    
    resource_name = body["metadata"]["name"]

    try:
        print(f"Attempting to create {resource_name}...")
        custom_api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            body=body
        )
        print(f"Successfully created {resource_name}")

    except ApiException as e:
        if e.status == 409:
            print(f"{resource_name} already exists. Patching (updating)...")
            try:
                custom_api.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=resource_name, 
                    body=body
                )
                print(f"Successfully updated {resource_name}")
            except ApiException as e_patch:
                print(f"Failed to patch: {e_patch}")
        else:
            raise e


@app.post("/sync")
def sync(request: dict):
    service = request['object']
    service_loadbalancer_label_value = service['metadata']['labels']['use-as-loadbalancer']
    dnsName = service['metadata']['labels']['gluekube-dns']
    dns_endpoint = {
        "apiVersion": "externaldns.k8s.io/v1alpha1",
        "kind": "DNSEndpoint",
        "metadata": {
            "name": f"dns-endpoint-{service['metadata']['name']}",
            "namespace": "default"
        },
        "spec": {
            "endpoints": [
                {
                    "recordTTL": 60,
                    "dnsName": dnsName,
                    "recordType": "A",
                    "targets": []
                }
            ]
        }
    }
    healthy_ips = []
    private_ips = []
    
    config.load_incluster_config()
  
    v1 = client.CoreV1Api()
    nodes = v1.list_node()

    print(f"Processing Service {service['metadata']['name']} with loadbalancer label value: {service_loadbalancer_label_value}")

    for node in nodes.items:
        # 1. Check if Node is Ready
        conditions = node.status.conditions
        is_ready = any(c.type == 'Ready' and c.status == 'True' for c in conditions)
        
        # 2. Check if Node has Traefik (optional but recommended)
        labels = node.metadata.labels
        match_lb = labels.get('use-as-loadbalancer') == service_loadbalancer_label_value
        
        # get internal ip
        
        if is_ready and match_lb:
            # 3. Extract Public IP
            pub_ip = labels['node-public-ip']
            privte_ip = labels['node-private-ip']

            if pub_ip:
                healthy_ips.append(pub_ip)
                private_ips.append(privte_ip)
    
    print(f"Healthy IPs for Service {service['metadata']['name']}: {healthy_ips}")
    dns_endpoint['spec']['endpoints'][0]['targets'] = healthy_ips
    apply_custom_object(
        group=group,
        version=version,
        namespace=namespace,
        plural=plural,
        body=dns_endpoint
    )
    service_ports: list[client.V1ServicePort] = []
    for port in service['spec']['ports']:
        service_ports.append(
            client.V1ServicePort(
                name=port['name'],
                port=port['port'],
                protocol=port['protocol'],
                target_port=port['targetPort']
            )
        )
    internal_service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(
            name=f"{service['metadata']['name']}-internal",
            namespace=service['metadata']['namespace'],
        ),
        spec=client.V1ServiceSpec(
            selector=service['spec']['selector'],
            ports=service_ports,
            type="ClusterIP",
            external_i_ps=healthy_ips+private_ips,
            external_traffic_policy=service['spec'].get('externalTrafficPolicy'),
            ip_family_policy=service['spec'].get('ipFamilyPolicy'),
            session_affinity=service['spec'].get('sessionAffinity')
        )
    )
    try:
        v1.create_namespaced_service(
            namespace=service['metadata']['namespace'],
            body=internal_service
        )
        print(f"Successfully created internal service {internal_service.metadata.name}")
    except ApiException as e:
        if e.status == 409:
            print(f"Internal service {internal_service.metadata.name} already exists. Patching (updating)...")
            try:
                v1.patch_namespaced_service(
                    name=internal_service.metadata.name,
                    namespace=service['metadata']['namespace'],
                    body=internal_service
                )
                print(f"Successfully updated internal service {internal_service.metadata.name}")
            except ApiException as e_patch:
                print(f"Failed to patch internal service: {e_patch}")
        else:
            print(f"Failed to create internal service: {e}")
            raise e
    # 4. Return the Status Update
    return {
        "status": {
            "loadBalancer": {
                "ingress": [
                    {"hostname": dnsName}
                ]
            }
        }
    }




@app.post("/finalize")
def finilize(request: dict):
    service = request['object']
    dns_endpoint_name = f"dns-endpoint-{service['metadata']['name']}"
    
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    custom_api = client.CustomObjectsApi()
    try:
        custom_api.delete_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            name=dns_endpoint_name
        )
        print(f"Successfully deleted {dns_endpoint_name}")
    except ApiException as e:
        if e.status == 404:
            print(f"{dns_endpoint_name} not found. Nothing to delete.")
        else:
            print(f"Failed to delete {dns_endpoint_name}: {e}")
            raise e
    result = v1.delete_namespaced_service(
        name=f"{service['metadata']['name']}-internal",
        namespace=service['metadata']['namespace']
    )
    print(result)
    print(f"Successfully deleted internal service {service['metadata']['name']}-internal")
    return {
        "status": {
            "loadBalancer": {
                "ingress": []
            }
        }
    }


