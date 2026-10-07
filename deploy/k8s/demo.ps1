# Demonstrație Kubernetes pe o singură mașină (kind). Rulează din rădăcina proiectului:
#   powershell -ExecutionPolicy Bypass -File deploy/k8s/demo.ps1 [-Step all|up|failover|scale|down]
param([string]$Step = "all")
$ErrorActionPreference = "Stop"
$ns = "padlabs"

function Wait-Leader {
    Write-Host "Aștept un pod broker 'Ready' (liderul)..." -ForegroundColor Cyan
    # standby-ul nu devine niciodată Ready (readiness = "sunt lider"), deci așteptăm DOAR un pod Ready
    $deadline = (Get-Date).AddSeconds(150)
    do {
        Start-Sleep -Seconds 2
        $ready = kubectl -n $ns get pods -l app=broker -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' 2>$null
    } while (-not ($ready -match "true") -and (Get-Date) -lt $deadline)
    foreach ($i in 0, 1) {
        $role = kubectl -n $ns exec "broker-$i" -- bash -c "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /role HTTP/1.0\r\n\r\n' >&3; tail -n1 <&3" 2>$null
        Write-Host "  broker-$i : $role"
    }
}

function Up {
    docker build -t padlabs/broker:latest ./broker
    docker build -t padlabs/clients:latest ./clients
    if (-not (kind get clusters | Select-String "^padlabs$")) {
        kind create cluster --config deploy/kind-cluster.yaml
    }
    kind load docker-image padlabs/broker:latest padlabs/clients:latest --name padlabs
    kubectl apply -f deploy/k8s/namespace.yaml
    kubectl apply -f deploy/k8s/
    Wait-Leader
    kubectl -n $ns get pods -o wide
}

function Failover {
    Write-Host "`n== Cădere lider: șterg pod-ul care este lider ==" -ForegroundColor Yellow
    $leader = $null
    foreach ($i in 0, 1) {
        $r = kubectl -n $ns exec "broker-$i" -- bash -c "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /role HTTP/1.0\r\n\r\n' >&3; tail -n1 <&3" 2>$null
        if ($r -match "leader") { $leader = "broker-$i" }
    }
    Write-Host "Lider curent: $leader"
    kubectl -n $ns delete pod $leader --wait=false
    Start-Sleep -Seconds 8
    Wait-Leader
    Write-Host "Loguri receiver (fluxul continuă):" -ForegroundColor Cyan
    kubectl -n $ns logs deploy/receiver --tail=5
}

function Scale {
    kubectl -n $ns scale deploy/receiver --replicas=3
    kubectl -n $ns rollout status deploy/receiver
    Start-Sleep -Seconds 6
    kubectl -n $ns logs -l app=receiver --tail=3 --prefix
}

function Down { kind delete cluster --name padlabs }

switch ($Step) {
    "up"       { Up }
    "failover" { Failover }
    "scale"    { Scale }
    "down"     { Down }
    default    { Up; Scale; Failover }
}
