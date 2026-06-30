# Notebook: alert_manager | Language: python | Commands: 2

# ===== CMD 1 =====
"""
alert_manager — Multi-channel notification manager.

Supports:
  - Microsoft Teams (Adaptive Card via webhook)
  - Slack (Block Kit via webhook)
  - Email (SMTP via Python smtplib — or Databricks SendGrid)
  - PagerDuty (Events API v2)

All webhook URLs are stored in Databricks Secret Scopes.
The AlertManager reads routing from notification_config table.

Usage:
    %run ../09_Monitoring/alert_manager
    am = AlertManager(spark, sm)
    am.notify_failure(config_row, "Connection timed out", run_id)
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class AlertManager:
    """
    Sends notifications for pipeline success, failure, and SLA breach.
    Each notification is non-blocking — failures are logged but never
    raise an exception that would stop the pipeline.
    """

    TIMEOUT_SEC = 10   # HTTP timeout for webhook calls

    def __init__(self, spark, secrets_manager):
        self.spark = spark
        self._sm   = secrets_manager
        self._cache: Dict[int, Any] = {}   # notification_id → config row

    # ------------------------------------------------------------------
    # Public notification triggers
    # ------------------------------------------------------------------

    def notify_failure(
        self,
        config_row:    Any,
        error_message: str,
        run_id:        str,
        attempt:       int = 1
    ) -> None:
        """Send failure alert. Called by orchestrator on any FAILED run."""
        notif = self._get_notification_config(config_row)
        if not notif or not notif.get("notify_on_failure"):
            return
        if not self._meets_severity(notif, "ERROR"):
            return

        title   = f"❌ Pipeline FAILED: {_get(config_row, 'pipeline_name')}"
        body    = {
            "Pipeline":    str(_get(config_row, "pipeline_name") or ""),
            "Source":      str(_get(config_row, "source_object")  or ""),
            "Target":      f"{_get(config_row,'target_catalog')}.{_get(config_row,'target_schema')}.{_get(config_row,'target_table')}",
            "Mode":        str(_get(config_row, "ingestion_mode") or ""),
            "Attempt":     f"{attempt}/{_get(config_row,'retry_max_attempts') or 3}",
            "Error":       error_message[:500] if error_message else "",
            "Run ID":      run_id,
            "Timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        self._dispatch(notif, title, body, severity="ERROR")

    def notify_success(
        self,
        config_row:   Any,
        rows_written: int,
        duration_sec: float,
        run_id:       str
    ) -> None:
        """Send success alert (only if notify_on_success=True in config)."""
        notif = self._get_notification_config(config_row)
        if not notif or not notif.get("notify_on_success"):
            return

        title = f"✅ Pipeline SUCCESS: {_get(config_row, 'pipeline_name')}"
        body  = {
            "Pipeline":    str(_get(config_row, "pipeline_name") or ""),
            "Rows written":f"{rows_written:,}",
            "Duration":    f"{duration_sec:.1f}s",
            "Run ID":      run_id,
            "Timestamp":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        self._dispatch(notif, title, body, severity="INFO")

    def notify_sla_breach(
        self,
        config_row:    Any,
        elapsed_min:   float,
        run_id:        str
    ) -> None:
        """Alert when a pipeline exceeds its SLA threshold."""
        notif = self._get_notification_config(config_row)
        if not notif or not notif.get("notify_on_sla_breach"):
            return

        sla     = int(_get(config_row, "sla_minutes") or 120)
        title   = f"⏰ SLA Breach: {_get(config_row, 'pipeline_name')}"
        body    = {
            "Pipeline":    str(_get(config_row, "pipeline_name") or ""),
            "SLA":         f"{sla} min",
            "Elapsed":     f"{elapsed_min:.1f} min",
            "Overage":     f"{elapsed_min - sla:.1f} min",
            "Run ID":      run_id,
        }
        self._dispatch(notif, title, body, severity="WARN")

    def notify_schema_drift(
        self,
        config_row:   Any,
        drift_count:  int,
        breaking:     bool,
        run_id:       str
    ) -> None:
        """Alert when schema drift is detected."""
        notif = self._get_notification_config(config_row)
        if not notif:
            return

        severity = "CRITICAL" if breaking else "WARN"
        if not self._meets_severity(notif, severity):
            return

        title = (
            f"🚨 Breaking Schema Drift: {_get(config_row, 'pipeline_name')}"
            if breaking else
            f"⚠️ Schema Drift: {_get(config_row, 'pipeline_name')}"
        )
        body = {
            "Pipeline":    str(_get(config_row, "pipeline_name") or ""),
            "Target":      f"{_get(config_row,'target_catalog')}.{_get(config_row,'target_schema')}.{_get(config_row,'target_table')}",
            "Changes":     str(drift_count),
            "Breaking":    str(breaking),
            "Action":      "Pipeline halted — manual review required" if breaking else "Auto-merged safe additions",
            "Run ID":      run_id,
        }
        self._dispatch(notif, title, body, severity=severity)

    # ------------------------------------------------------------------
    # Channel dispatchers
    # ------------------------------------------------------------------

    def _dispatch(
        self,
        notif:    Dict,
        title:    str,
        body:     Dict,
        severity: str = "INFO"
    ) -> None:
        """Route to the appropriate channel(s)."""
        channel = (notif.get("channel") or "").lower()
        try:
            if channel == "teams":
                self._send_teams(notif, title, body, severity)
            elif channel == "slack":
                self._send_slack(notif, title, body, severity)
            elif channel == "email":
                self._send_email(notif, title, body)
            elif channel == "pagerduty":
                self._send_pagerduty(notif, title, body, severity)
            else:
                print(f"[AlertManager] No channel configured (channel={channel}).")
        except Exception as exc:
            # Notification failures must NEVER propagate to the pipeline
            print(f"[AlertManager] WARNING: Notification to {channel} failed: {exc}")

    def _send_teams(
        self, notif: Dict, title: str, body: Dict, severity: str
    ) -> None:
        """Post an Adaptive Card to a Teams channel via incoming webhook."""
        import requests
        webhook_url = self._sm.get_secret(
            notif["secret_scope"] if "secret_scope" in notif else "",
            notif.get("webhook_secret_key") or ""
        ) if notif.get("webhook_secret_key") else ""

        if not webhook_url:
            return

        color_map  = {"INFO": "Good", "WARN": "Warning", "ERROR": "Attention", "CRITICAL": "Attention"}
        themeColor = {"INFO": "00b050", "WARN": "ff9900", "ERROR": "d40000", "CRITICAL": "d40000"}

        # Teams MessageCard format
        payload = {
            "@type":      "MessageCard",
            "@context":   "https://schema.org/extensions",
            "summary":    title,
            "themeColor": themeColor.get(severity, "0078d4"),
            "title":      title,
            "sections":   [{
                "facts": [
                    {"name": k, "value": str(v)}
                    for k, v in body.items()
                ]
            }]
        }
        resp = requests.post(
            webhook_url,
            json=payload,
            timeout=self.TIMEOUT_SEC
        )
        resp.raise_for_status()

    def _send_slack(
        self, notif: Dict, title: str, body: Dict, severity: str
    ) -> None:
        """Post a Block Kit message to a Slack channel via incoming webhook."""
        import requests
        webhook_url = self._sm.get_secret(
            notif.get("secret_scope", ""),
            notif.get("webhook_secret_key") or ""
        ) if notif.get("webhook_secret_key") else ""

        if not webhook_url:
            return

        emoji_map = {"INFO": ":white_check_mark:", "WARN": ":warning:",
                     "ERROR": ":x:", "CRITICAL": ":rotating_light:"}
        emoji = emoji_map.get(severity, ":information_source:")

        fields = [{
            "type": "mrkdwn",
            "text": f"*{k}*: {v}"
        } for k, v in body.items()]

        payload = {
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {title}"}},
                {"type": "section", "fields": fields[:10]}
            ]
        }
        resp = requests.post(webhook_url, json=payload, timeout=self.TIMEOUT_SEC)
        resp.raise_for_status()

    def _send_email(self, notif: Dict, title: str, body: Dict) -> None:
        """Placeholder for email via SMTP or Databricks SendGrid integration."""
        email_to = notif.get("email_to") or ""
        if email_to:
            print(f"[AlertManager] Email alert '{title}' would be sent to: {email_to}")
            # TODO: Implement SMTP or SendGrid here when email server is configured

    def _send_pagerduty(
        self, notif: Dict, title: str, body: Dict, severity: str
    ) -> None:
        """Send a PagerDuty Events API v2 trigger."""
        import requests
        routing_key = self._sm.get_secret_or_none(
            notif.get("secret_scope", ""),
            notif.get("webhook_secret_key")
        )
        if not routing_key:
            return

        sev_map = {"INFO": "info", "WARN": "warning", "ERROR": "error", "CRITICAL": "critical"}
        payload = {
            "routing_key":  routing_key,
            "event_action": "trigger",
            "payload": {
                "summary":  title,
                "severity": sev_map.get(severity, "error"),
                "source":   "Databricks Ingestion Framework",
                "custom_details": body
            }
        }
        requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload, timeout=self.TIMEOUT_SEC
        ).raise_for_status()

    # ------------------------------------------------------------------
    # Config loader
    # ------------------------------------------------------------------

    def _get_notification_config(self, config_row: Any) -> Optional[Dict]:
        """Load notification_config row for this pipeline."""
        notif_id = _get(config_row, "notification_id")
        if not notif_id:
            return None
        if notif_id in self._cache:
            return self._cache[notif_id]
        try:
            row = (
                self.spark.table(TBL_NOTIFICATION_CONFIG)
                .filter(self.spark.sql(f"notification_id = {notif_id} AND active = true"))
                .first()
            )
            if row:
                d = row.asDict()
                self._cache[notif_id] = d
                return d
        except Exception:
            pass
        return None

    @staticmethod
    def _meets_severity(notif: Dict, severity: str) -> bool:
        order = {"INFO": 0, "WARN": 1, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
        min_sev = (notif.get("min_severity") or "ERROR").upper()
        return order.get(severity.upper(), 2) >= order.get(min_sev, 2)


print("[alert_manager] Loaded — AlertManager ready (Teams, Slack, email, PagerDuty).")

# ===== CMD 2 =====


