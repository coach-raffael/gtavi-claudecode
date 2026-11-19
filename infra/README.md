# GameInsight Clip Desk auf dem VPS

Der Clip Desk läuft als eigener Docker-Stack `gameinsight` auf dem Hostinger-VPS neben der
language-platform. Er bietet eine kleine HTTP-API (`server/clipdesk.py`), über die n8n die Skripte
stündlich startet. Gepostet wird nichts automatisch: Die Clips kommen zur Freigabe per Telegram.

```
n8n (stündlich) ──POST /pipeline──▶ clipdesk ──▶ trends → moments → cut (+Hook via Claude) → check
       ▲                                │
       └──── GET /runs/<id>, /files ◀───┘  ──▶ Telegram: Clip + Caption + Regelcheck
```

## Einmalig einrichten

1. **DNS:** A-Record `clips.coach-raffael.com` → IP des VPS (Traefik holt das Zertifikat selbst).
2. **Secrets und Variablen im Fork** (`Settings → Environments → production`). Repo-Secrets der
   language-platform gelten hier nicht, sie müssen ins Fork-Repo (oder als Organisations-Secret):

   | Typ | Name | Wert |
   |---|---|---|
   | Secret | `VPS_HOST` | wie bei language-platform |
   | Secret | `VPS_SSH_KEY` | wie bei language-platform |
   | Secret | `VPS_KNOWN_HOSTS` | wie bei language-platform (optional) |
   | Secret | `CLIPDESK_TOKEN` | neu, z. B. `openssl rand -hex 32` |
   | Secret | `ANTHROPIC_API_KEY` | für die Hooks (ohne: Clips ohne Hook) |
   | Variable | `DOMAIN` | `clips.coach-raffael.com` |
   | Variable | `SSH_USER` | wie bei language-platform (Standard `deploy`) |
   | Variable | `DEPLOY_ENABLED` | `true` = nach jedem Image-Build automatisch ausrollen |
   | Variable | `TRAEFIK_ENTRYPOINT` | optional, wie bei language-platform (Standard `websecure`) |
   | Variable | `TRAEFIK_CERTRESOLVER` | optional, wie bei language-platform (Standard `letsencrypt`) |
   | Variable | `HOOK_MODEL` / `RETAIN_DAYS` | optional (Standard `claude-haiku-5-5` / `3`) |

   Router und Service heißen `gameinsight-clipdesk`, damit nichts mit anderen Stacks kollidiert.
   Läuft Traefik nicht im Host-Netz, erkennt der Deploy sein Docker-Netz und legt
   `compose.override.yml` an, die den Container dort einhängt.

3. **Actions im Fork aktivieren** (bei Forks standardmäßig aus): Tab „Actions“ → „I understand…“.
4. **Erstes Rollout:** Actions → `clipdesk-image` → Run workflow, danach `clipdesk-deploy` → Run workflow.
5. **n8n:** `infra/n8n-gameinsight-workflow.json` importieren, dann
   - Credential „Clipdesk Token“ anlegen: *Header Auth*, Name `Authorization`, Wert `Bearer <CLIPDESK_TOKEN>`
   - Credential „Telegram GameInsight“ (Bot-Token) anlegen
   - im Node **Config** `base_url` und `telegram_chat_id` eintragen
   - erst nach einem erfolgreichen Testlauf aktivieren.

Startet der Container während eines Laufs neu (Deploy, OOM), wird der Lauf beim Hochfahren als
`failed` markiert, damit n8n nicht endlos weiter abfragt.

## Ressourcen

KVM 4 hat 4 vCPU / 16 GB. Der Container ist auf 3 CPUs und 8 GB begrenzt, damit die
language-platform flüssig bleibt. Daten liegen unter `/srv/gameinsight/data` (Clips, Reports,
Whisper-Modell ≈ 0,5 GB). Job-Ordner mit Quellvideos werden nach `RETAIN_DAYS` (Standard 3) gelöscht.

## API von Hand testen

```bash
T="Authorization: Bearer $CLIPDESK_TOKEN"; B=https://clips.coach-raffael.com
curl $B/health
curl -H "$T" -X POST $B/pipeline -d '{"clips":2,"layout":"blur"}'      # → {"run": "..."}
curl -H "$T" $B/runs/<id>                                              # Status, Log, Clips mit URL
curl -H "$T" -X POST $B/jobs/streams -d '{"args":["--query","gta 6"]}'  # wer streamt gerade
curl -H "$T" -X POST $B/jobs/live -d '{"args":["https://twitch.tv/<login>","--minutes","120"]}'
```

## Bekanntes Risiko

YouTube kann Downloads von Rechenzentrums-IPs mit „Sign in to confirm you're not a bot“ blocken.
Dann scheitert `trends`/`moments` im Log. Twitch und Kick sind davon meist nicht betroffen.
