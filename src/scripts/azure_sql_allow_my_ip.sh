#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# WIZARD BANK · Habilitar la IP actual en el firewall de Azure SQL
# Data Wizard Academy — proyecto integrador
#
# Azure SQL bloquea por defecto cualquier IP no autorizada. Correr esto cada vez
# que cambie la IP pública local (red distinta, VPN, etc.) para poder conectarse
# al servidor con el generador de datos, notebooks locales, etc.
#
# Uso:
#     bash azure_sql_allow_my_ip.sh
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

RESOURCE_GROUP="rg-wizardbank"
SQL_SERVER="sql-wizardbank-dev"
MY_IP=$(curl -s ifconfig.me)

az sql server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" --server "$SQL_SERVER" \
  --name AllowMyIP --start-ip-address "$MY_IP" --end-ip-address "$MY_IP"
