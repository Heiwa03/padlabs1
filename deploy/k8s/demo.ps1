# Demonstrație Kubernetes pe o singură mașină (kind). Rulează din rădăcina proiectului:
#   powershell -ExecutionPolicy Bypass -File deploy/k8s/demo.ps1 [-Step all|up|failover|scale|down]
param([string]$Step = "all")
# "Continue": în PowerShell 5.1, cu "Stop", orice stderr redirecționat (2>$null) al unei comenzi native devine excepție
$ErrorActionPreference = "Continue"
$ns = "padlabs"

function Wait-Leader {
    Write-Host "Aștept un pod broker 'Ready' (liderul)..." -ForegroundColor Cyan
    # standby-ul nu devine niciodată Ready (readiness = "sunt lider"), deci așteptăm DOAR un pod Ready
    $deadline = (Get-Date).AddSeconds(150)
    do {
        Start-Sleep -Seconds 2
        $ready = kubectl -n $ns get pods -l app=broker -o jsonpath='{.items[*].status.containerStatuses[0].ready}' 2>$null
    } while (-not ($ready -match "true") -and (Get-Date) -lt $deadline)
    foreach ($i in 0, 1) {
        $role = kubectl -n $ns exec "broker-$i" -- bash -c "exec 3<>/dev/tcp/127.0.0.1/8080; printf 'GET /role HTTP/1.0\r\n\r\n' >&3; tail -n1 <&3" 2>$null
        Write-Host "  broker-$i : $role"
    }
}

function Up {
    docker build -t padlabs/broker:latest ./broker;   if ($LASTEXITCODE) { throw "docker build broker a eșuat" }
    docker build -t padlabs/clients:latest ./clients; if ($LASTEXITCODE) { throw "docker build clients a eșuat" }
    if (-not (kind get clusters 2>$null | Select-String "^padlabs$")) {
        kind create cluster --config deploy/kind-cluster.yaml
        if ($LASTEXITCODE) { throw "kind create cluster a eșuat" }
    }
    kind load docker-image padlabs/broker:latest padlabs/clients:latest --name padlabs
    if ($LASTEXITCODE) { throw "kind load docker-image a eșuat" }
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
    $node = kubectl -n $ns get pod $leader -o jsonpath='{.spec.nodeName}'
    Write-Host "Lider curent: $leader (nod $node)"
    # Fără cordon, StatefulSet-ul recreează pod-ul în ~1 s (sub pragul de failover) și liderul doar repornește.
    # Cu nodul "cordoned", pod-ul rămâne Pending (PVC-ul local-path e legat de nod) -> standby-ul preia conducerea.
    kubectl cordon $node
    kubectl -n $ns delete pod $leader --wait=false
    Start-Sleep -Seconds 8
    Wait-Leader
    Write-Host "Loguri receiver (fluxul continuă):" -ForegroundColor Cyan
    kubectl -n $ns logs -l app=receiver --tail=2 --prefix

    Write-Host "`n== Revenire: uncordon $node; vechiul lider pornește și devine standby ==" -ForegroundColor Yellow
    kubectl uncordon $node
    kubectl -n $ns wait pod/$leader --for=jsonpath='{.status.phase}'=Running --timeout=90s
    Start-Sleep -Seconds 6
    Wait-Leader
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
