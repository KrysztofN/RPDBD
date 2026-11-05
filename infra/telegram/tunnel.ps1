# Tunel dla bota Telegram: sieć klastra blokuje api.telegram.org, więc ruch bota idzie przez ten laptop.
#
# Co robi: ssh -R otwiera na masterze (tylko 127.0.0.1:1080) serwer SOCKS, którego ruch wychodzi w świat przez
# laptop. Bot (TELEGRAM_PROXY=socks5h://127.0.0.1:1080) używa go tylko do Telegrama; ClickHouse/HDFS/Kafka bez zmian.
# Po zerwaniu (VPN, uśpienie) łączy się ponownie co 15 s. Zatrzymanie: Ctrl+C (albo zamknięcie okna).
# Wymaga: VPN do klastra, alias `rbd` w ~/.ssh/config (klucz w ssh-agent).
#
# Uruchomienie (PowerShell na laptopie):
#   powershell -ExecutionPolicy Bypass -File c:\sem7\RBD\infra\telegram\tunnel.ps1
# Sprawdzenie na masterze:  ssh rbd "ss -ltn | grep 1080"

while ($true) {
    Write-Host "$(Get-Date -Format HH:mm:ss) tunel: łączę z rbd (127.0.0.1:1080 na masterze)..."
    ssh -N -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes -R 127.0.0.1:1080 rbd
    Write-Host "$(Get-Date -Format HH:mm:ss) tunel: rozłączony, ponawiam za 15 s (Ctrl+C = koniec)"
    Start-Sleep -Seconds 15
}
