import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date, datetime
import openpyxl
from io import BytesIO

# ==========================================
# CONFIG STREAMLIT
# ==========================================
st.set_page_config(page_title="Gestão de Colaboradores", layout="wide")

# ==========================================
# CONEXÃO COM POSTGRESQL (NEON)
# ==========================================
import os

DATABASE_URL = st.secrets["ConnectDB"]

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# ==========================================
# CRIAÇÃO DAS TABELAS
# ==========================================

# ----- TABELA COLABORADORES -----
cursor.execute("""
CREATE TABLE IF NOT EXISTS colaboradores (
    id SERIAL PRIMARY KEY,
    nome TEXT,
    cpf TEXT,
    unidade TEXT,
    cargo TEXT,
    telefone TEXT,
    endereco TEXT,
    salario_base_cents INTEGER,
    conta_deposito TEXT,
    nascimento DATE,
    admissao DATE,
    demissao DATE,
    ativo BOOLEAN DEFAULT TRUE
)
""")

# ----- TABELA FOLHA PAGAMENTO -----
cursor.execute("""
CREATE TABLE IF NOT EXISTS folha_pagamento (
    id SERIAL PRIMARY KEY,
    colaborador_id INTEGER,
    colaborador_nome TEXT,
    cpf TEXT,
    unidade TEXT,
    mes_referencia DATE,
    salario_base_cents INTEGER,
    valor_depositado_cents INTEGER,
    conta_deposito TEXT,
    data_pagamento DATE,
    horas_extras_cents INTEGER,
    faltas_cents INTEGER,
    bonus_cents INTEGER,
    comissoes_cents INTEGER,
    descontos_cents INTEGER,
    observacoes TEXT
)
""")

conn.commit()

# --------------------------
# Bloco 2 - Funções utilitárias
# --------------------------
def to_date_or_none(s):
    """
    Converte string 'YYYY-MM-DD' para objeto date. Retorna None se inválido.
    """
    try:
        if not s or str(s).strip() == "":
            return None
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except:
        return None


def safe_parse_date(s):
    """
    Igual a to_date_or_none, mas mais robusto para diferentes tipos.
    """
    try:
        if not s or str(s).strip() == "":
            return None
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except Exception:
        return None


def cents_to_real(c):
    """
    Converte centavos (int) para reais (float 2 casas decimais).
    Se None, retorna 0.0
    """
    if c is None:
        return 0.0
    return round(c / 100.0, 2)


def real_to_cents(r):
    """
    Converte reais (float) para centavos (int)
    """
    if r is None:
        return 0
    return int(round(float(r) * 100))


def read_df(query_or_where=None, params=None):
    """
    Lê dados do PostgreSQL usando pandas.
    Se query_or_where for uma string longa com SELECT, executa direto.
    Se query_or_where for cláusula WHERE (ex: 'ativo=1'), constrói SELECT * FROM colaboradores WHERE ...
    """
    if query_or_where and query_or_where.strip().upper().startswith("SELECT"):
        q = query_or_where
    else:
        q = "SELECT * FROM colaboradores"
        if query_or_where:
            q += " WHERE " + query_or_where

    df = pd.read_sql_query(q, conn, params=params or [])
    if df.empty:
        return df

    # garantir tipos corretos
    if "ativo" in df.columns:
        df["ativo"] = df["ativo"].fillna(0).astype(int)
    if "salario_cents" in df.columns:
        df["salario_cents"] = df["salario_cents"].fillna(0).astype(int)
        df["salario_reais"] = df["salario_cents"] / 100.0
    return df

# =========================================================
# Bloco 3 - FOLHA DE PAGAMENTO
# =========================================================
if pagina == "Folha de Pagamento":
    st.title("💼 Folha de Pagamento")

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        unidade_sel = st.selectbox(
            "Unidade (filtrar)", options=["(Todas)"] + UNIDADES, index=0
        )
    with col2:
        mes_input = st.date_input(
            "Mês de referência (escolha qualquer dia desse mês)",
            value=date.today().replace(day=1)
        )
        mes_ref = date(mes_input.year, mes_input.month, 1)
    with col3:
        st.write("")  # espaço

    # -------------------------
    # Gerar lançamentos
    # -------------------------
    if st.button("Gerar lançamentos para unidade/mês"):
        if unidade_sel == "(Todas)":
            q_col = "SELECT id, nome, salario_cents, conta_deposito, cpf, unidade FROM colaboradores"
            params = ()
        else:
            q_col = """
            SELECT id, nome, salario_cents, conta_deposito, cpf, unidade
            FROM colaboradores
            WHERE unidade = %s
            """
            params = (unidade_sel,)

        df_col = read_df(q_col, params)
        if df_col.empty:
            st.warning("Nenhum colaborador encontrado para gerar lançamentos.")
        else:
            inserted = 0
            for _, r in df_col.iterrows():
                colaborador_id = int(r["id"])
                colaborador_nome = r["nome"]
                cpf = r.get("cpf")
                conta = r.get("conta_deposito")
                salario_cents = int(r["salario_cents"]) if pd.notna(r["salario_cents"]) else 0
                unidade = r.get("unidade")

                # Verificar se já existe lançamento para este colaborador/mês
                cursor.execute("""
                    SELECT 1 FROM folha_pagamento
                    WHERE colaborador_id = %s AND mes_referencia = %s
                    LIMIT 1
                """, (colaborador_id, mes_ref))
                if cursor.fetchone():
                    continue

                # Inserir lançamento
                cursor.execute("""
                    INSERT INTO folha_pagamento (
                        colaborador_id, colaborador_nome, cpf, unidade, mes_referencia,
                        salario_base_cents, valor_depositado_cents, conta_deposito
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    colaborador_id, colaborador_nome, cpf, unidade, mes_ref,
                    salario_cents, None, conta
                ))
                inserted += 1
            conn.commit()
            st.success(f"{inserted} lançamentos gerados (não duplicados).")

    st.markdown("---")

    # -------------------------
    # Exibir lançamentos
    # -------------------------
    if unidade_sel == "(Todas)":
        df_f = read_df(
            "SELECT * FROM folha_pagamento WHERE mes_referencia = %s ORDER BY colaborador_nome",
            params=(mes_ref,)
        )
    else:
        df_f = read_df(
            "SELECT * FROM folha_pagamento WHERE mes_referencia = %s AND unidade = %s ORDER BY colaborador_nome",
            params=(mes_ref, unidade_sel)
        )

    if df_f.empty:
        st.info("Nenhum lançamento para o mês/unidade selecionados.")
    else:
        df_show = df_f.copy()
        df_show["salario_base_reais"] = df_show["salario_base_cents"].apply(cents_to_real)
        df_show["valor_depositado_reais"] = df_show["valor_depositado_cents"].apply(cents_to_real)
        df_show["mes_referencia_str"] = pd.to_datetime(df_show["mes_referencia"]).dt.strftime("%Y-%m")

        # Colunas padrão
        display_cols = [
            "id", "colaborador_nome", "salario_base_reais", "valor_depositado_reais",
            "conta_deposito", "mes_referencia_str", "data_pagamento", "cpf"
        ]
        st.dataframe(df_show[display_cols].rename(columns={
            "id": "ID",
            "colaborador_nome": "Nome",
            "salario_base_reais": "Salário base (R$)",
            "valor_depositado_reais": "Valor depositado (R$)",
            "conta_deposito": "Conta",
            "mes_referencia_str": "Mês",
            "data_pagamento": "Data Pagamento",
            "cpf": "CPF"
        }))

    # -------------------------
    # Exportar para Excel
    # -------------------------
    st.markdown("---")
    st.subheader("Exportar para Excel (.xlsx)")

    incluir_extras = st.checkbox(
        "Incluir colunas extras (horas_extras, bonus, descontos, observacoes)", value=False
    )

    if st.button("Exportar selecionados (XLSX)"):
        df_export = df_show.copy()
        df_export["valor_depositado_reais"] = df_export["valor_depositado_reais"].fillna(0)
        df_export["salario_base_reais"] = df_export["salario_base_reais"].fillna(0)
        df_export["mes_referencia"] = df_export["mes_referencia_str"]

        cols_order = [
            "id", "colaborador_nome", "valor_depositado_reais", "conta_deposito",
            "salario_base_reais", "mes_referencia", "data_pagamento", "cpf"
        ]
        rename_map = {
            "id": "ID",
            "colaborador_nome": "Nome",
            "valor_depositado_reais": "Valor depositado (R$)",
            "conta_deposito": "Conta de depósito",
            "salario_base_reais": "Salário base (R$)",
            "mes_referencia": "Mês referência",
            "data_pagamento": "Data pagamento",
            "cpf": "CPF"
        }

        df_out = df_export.copy()
        for c in cols_order:
            if c not in df_out.columns:
                df_out[c] = None
        df_out = df_out[cols_order].rename(columns=rename_map)

        if incluir_extras:
            extras = ["horas_extras_cents", "bonus_cents", "descontos_cents", "observacoes"]
            for e in extras:
                if e in df_export.columns:
                    df_out[e] = df_export[e]
                else:
                    df_out[e] = None
            for e in ["horas_extras_cents", "bonus_cents", "descontos_cents"]:
                if e in df_out.columns:
                    df_out[e.replace("_cents","")] = df_out[e].apply(cents_to_real)
                    df_out.drop(columns=[e], inplace=True)

        # Criar arquivo em memória
        import io
        towrite = io.BytesIO()
        with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
            df_out.to_excel(writer, index=False, sheet_name="Folha")
        towrite.seek(0)

        st.download_button(
            label="⬇️ Baixar Excel",
            data=towrite,
            file_name=f"folha_{mes_ref.strftime('%Y_%m')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# =========================================================
# Bloco 4 - RELATÓRIOS E ESTATÍSTICAS
# =========================================================
elif pagina == "Relatórios e Estatísticas":
    st.title("📊 Relatórios e Estatísticas")

    df = read_df()
    if df.empty:
        st.info("Nenhum dado cadastrado ainda.")
    else:
        # --- Filtros ---
        st.sidebar.markdown("### Filtros (Relatórios)")
        sel_unidades = st.sidebar.multiselect(
            "Unidades",
            options=sorted(df["unidade"].dropna().unique()),
            default=sorted(df["unidade"].dropna().unique())
        )
        sel_status = st.sidebar.multiselect(
            "Status",
            options=["Ativos", "Inativos"],
            default=["Ativos", "Inativos"]
        )

        df_r = df.copy()
        if sel_unidades:
            df_r = df_r[df_r["unidade"].isin(sel_unidades)]
        if "Ativos" in sel_status and "Inativos" not in sel_status:
            df_r = df_r[df_r["ativo"] == 1]
        elif "Inativos" in sel_status and "Ativos" not in sel_status:
            df_r = df_r[df_r["ativo"] == 0]

        # Garantir parsing de datas
        df_r["admissao_parsed"] = pd.to_datetime(df_r["admissao"], errors="coerce")
        df_r["saida_parsed"] = pd.to_datetime(df_r["saida"], errors="coerce")

        today = pd.to_datetime(date.today())

        # --------------------
        # Antiguidade / Tempo de Casa
        # --------------------
        df_r["tenure_days"] = (today - df_r["admissao_parsed"]).dt.days
        tenure_valid = df_r.dropna(subset=["admissao_parsed"]).copy()
        avg_by_unit = tenure_valid.groupby("unidade")["tenure_days"].mean().reset_index()

        def format_days_to_years_months(d):
            if pd.isna(d):
                return "-"
            years = int(d // 365)
            months = int((d % 365) // 30)
            return f"{years}a {months}m"

        avg_by_unit["media_tempo"] = avg_by_unit["tenure_days"].apply(format_days_to_years_months)

        st.subheader("⏳ Tempo de Casa (Antiguidade)")
        st.markdown("**Média de tempo de casa por unidade**")
        st.table(avg_by_unit[["unidade", "media_tempo"]].rename(columns={"unidade":"Unidade","media_tempo":"Média"}))

        st.markdown("**Top 10 mais antigos**")
        top10 = tenure_valid.sort_values("tenure_days", ascending=False).head(10)
        if not top10.empty:
            top10_display = top10[["id","nome","unidade","tenure_days","admissao_parsed"]].copy()
            top10_display["tempo"] = top10_display["tenure_days"].apply(format_days_to_years_months)
            st.dataframe(top10_display[["id","nome","unidade","tempo","admissao_parsed"]].rename(columns={"admissao_parsed":"Admissão"}))
        else:
            st.info("Nenhuma admissão válida encontrada para calcular antiguidade.")

        st.markdown("**Pessoas com menos de 3 meses (novatos)**")
        novatos = tenure_valid[tenure_valid["tenure_days"] < 90]
        if not novatos.empty:
            st.dataframe(novatos[["id","nome","unidade","tenure_days","admissao_parsed"]].rename(columns={"admissao_parsed":"Admissão","tenure_days":"Dias de casa"}))
        else:
            st.info("Nenhum novato (menos de 3 meses) encontrado.")

        # --------------------
        # Folha Total por Unidade
        # --------------------
        st.subheader("💰 Folha Total por Unidade")
        folha_unit = df_r.groupby("unidade")["salario_reais"].sum().reset_index().sort_values("salario_reais", ascending=False)
        folha_unit = folha_unit.rename(columns={"salario_reais":"folha_total"})
        st.dataframe(folha_unit)

        import plotly.express as px
        if not folha_unit.empty:
            fig_folha = px.pie(folha_unit, names="unidade", values="folha_total", title="Distribuição da folha por unidade")
            st.plotly_chart(fig_folha, use_container_width=True)

        # --------------------
        # Alertas Automáticos (qualidade de dados)
        # --------------------
        st.subheader("🚨 Alertas Automáticos (Qualidade de Dados)")

        def show_alert(title, df_alert, extra_cols):
            base = ["id", "nome", "unidade"]
            cols = base + extra_cols
            cols = [c for c in cols if c in df_alert.columns]
            st.markdown(f"**{title}** — {len(df_alert)}")
            st.dataframe(df_alert[cols].head(200))

        nasc_ou_adm_sem_data = df_r[(df_r["nascimento"].fillna("").str.strip() == "") | (df_r["admissao_parsed"].isna())]
        if not nasc_ou_adm_sem_data.empty:
            show_alert("Nascimento/Admissão sem data", nasc_ou_adm_sem_data, ["nascimento", "admissao"]) 

        salarios_zerados = df_r[df_r["salario_cents"] == 0]
        if not salarios_zerados.empty:
            show_alert("Salário zerado", salarios_zerados, ["salario_reais"]) 

        faltando_doc = df_r[(df_r["cpf"].fillna("").str.strip() == "") | (df_r["rg_outro"].fillna("").str.strip() == "") | (df_r["emissao"].fillna("").str.strip() == "")]
        if not faltando_doc.empty:
            show_alert("Faltando CPF/RG/Emissão", faltando_doc, ["cpf", "rg_outro", "emissao"]) 

        phone_pattern = re.compile(r'^\(\d{2}\)\s?\d{4,5}-\d{4}$')
        invalid_phone = df_r[df_r["telefone"].fillna("").apply(lambda x: not bool(phone_pattern.match(x)))]
        if not invalid_phone.empty:
            show_alert("Telefone inválido", invalid_phone, ["telefone"]) 

        inativo_sem_saida = df_r[(df_r["ativo"] == 0) & (df_r["saida_parsed"].isna())]
        if not inativo_sem_saida.empty:
            show_alert("Inativo sem data de saída", inativo_sem_saida, ["saida"]) 

        conta_vazia = df_r[df_r["conta_deposito"].fillna("").str.strip() == ""]
        if not conta_vazia.empty:
            show_alert("Conta de depósito vazia", conta_vazia, ["conta_deposito"]) 

        faltando_sociais = df_r[(df_r["estado_civil"].fillna("").str.strip() == "") | (df_r["escolaridade"].fillna("").str.strip() == "") | (df_r["naturalidade"].fillna("").str.strip() == "")]
        if not faltando_sociais.empty:
            show_alert("Faltando dados sociais", faltando_sociais, ["estado_civil", "escolaridade", "naturalidade"]) 

        faltando_endereco = df_r[(df_r["cep"].fillna("").str.strip() == "") | (df_r["bairro"].fillna("").str.strip() == "") | (df_r["endereco"].fillna("").str.strip() == "")]
        if not faltando_endereco.empty:
            show_alert("Endereço incompleto", faltando_endereco, ["cep", "bairro", "endereco"]) 

        if (nasc_ou_adm_sem_data.empty and salarios_zerados.empty and faltando_doc.empty and
            invalid_phone.empty and inativo_sem_saida.empty and conta_vazia.empty and
            faltando_sociais.empty and faltando_endereco.empty):
            st.success("Nenhum problema de qualidade de dados detectado!")

        # --------------------
        # Dashboard Comparativo Entre Unidades
        # --------------------
        st.subheader("📊 Dashboard Comparativo Entre Unidades")
        comp = df_r.copy()
        comp["unidade"] = comp["unidade"].fillna("(Sem Unidade)")
        summary = comp.groupby("unidade").agg(
            Total=("id", "count"),
            Ativos=("ativo", lambda s: int((s==1).sum())),
            Media_Salarial=("salario_reais", lambda s: round(s.mean() if len(s)>0 else 0,2)),
            Folha=("salario_reais", "sum")
        ).reset_index()

        last_12 = today - pd.Timedelta(days=365)
        exits_12m = df_r[(df_r["saida_parsed"].notna()) & (df_r["saida_parsed"] >= last_12)]
        exits_count = exits_12m.groupby("unidade")["id"].count().reset_index().rename(columns={"id":"saidas_12m"})
        summary = summary.merge(exits_count, on="unidade", how="left")
        summary["saidas_12m"] = summary["saidas_12m"].fillna(0).astype(int)
        summary["Turnover"] = summary.apply(lambda r: round(r["saidas_12m"] / r["Total"], 3) if r["Total"]>0 else 0, axis=1)

        overall_avg_sal = summary["Media_Salarial"].mean()
        def sal_flag(x):
            if pd.isna(x):
                return "-"
            return "🟢 Acima" if x >= overall_avg_sal else "🔴 Abaixo"
        summary["Salario_vs_media"] = summary["Media_Salarial"].apply(sal_flag)

        st.dataframe(summary)

        # Exportar CSV
        csv = df_r.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar dados (CSV)", csv, file_name="colaboradores_filtrados.csv", mime="text/csv")

# =========================================================
# Bloco 5 - FERRAMENTAS EXTRAS / INTEGRAÇÕES
# =========================================================
elif pagina == "Ferramentas Extras":
    st.title("⚙️ Ferramentas Extras e Integrações")

    df = read_df()
    if df.empty:
        st.info("Nenhum dado cadastrado ainda.")
    else:
        # --------------------
        # Envio de e-mail (ex: aniversariantes do mês)
        # --------------------
        st.subheader("📧 Aniversariantes do Mês")
        today = pd.to_datetime(date.today())
        df["nascimento_parsed"] = pd.to_datetime(df["nascimento"], errors="coerce")
        aniversariantes = df[df["nascimento_parsed"].notna()]
        aniversariantes = aniversariantes[aniversariantes["nascimento_parsed"].dt.month == today.month]

        if not aniversariantes.empty:
            st.dataframe(aniversariantes[["nome","unidade","nascimento"]])
            st.info("Aqui você poderia integrar envio de e-mail de felicitações automaticamente.")
        else:
            st.info("Nenhum aniversariante neste mês.")

        # --------------------
        # Exportação Avançada
        # --------------------
        st.subheader("💾 Exportação Avançada")
        formato = st.radio("Formato do arquivo:", ["CSV", "Excel"])
        if st.button("Gerar arquivo"):
            if formato == "CSV":
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Baixar CSV", csv, file_name="colaboradores.csv", mime="text/csv")
            elif formato == "Excel":
                import io
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Colaboradores')
                    writer.save()
                st.download_button("⬇️ Baixar Excel", output.getvalue(), file_name="colaboradores.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # --------------------
        # Integração com APIs externas (ex: folha, ponto, banco)
        # --------------------
        st.subheader("🔗 Integrações com APIs externas")
        st.markdown(
            """
            Aqui você poderia conectar com:
            - Sistema de ponto online
            - Sistema de folha de pagamento
            - Banco para pagamentos automáticos
            - Qualquer outra API necessária
            """
        )
        st.info("Funcionalidades de integração dependem de credenciais e endpoints específicos.")

        # --------------------
        # Notificações / Lembretes
        # --------------------
        st.subheader("⏰ Notificações e Lembretes")
        st.markdown(
            """
            Exemplos de lembretes que poderiam ser implementados:
            - Avisar sobre aniversariantes
            - Avisar sobre contratos a vencer
            - Avisar sobre férias a iniciar
            - Envio de alerta via e-mail ou Telegram
            """
        )

# =========================================================
# Bloco 6 - CONFIGURAÇÕES / ADMIN
# =========================================================
elif pagina == "Configurações":
    st.title("⚙️ Configurações do Sistema")

    # --------------------
    # Usuários e Permissões
    # --------------------
    st.subheader("👥 Usuários e Permissões")
    st.markdown(
        """
        Aqui você poderia:
        - Criar novos usuários do sistema
        - Definir níveis de permissão (admin, RH, gerente)
        - Gerenciar senhas ou tokens de acesso
        """
    )

    # --------------------
    # Parâmetros Globais
    # --------------------
    st.subheader("🔧 Parâmetros Globais")
    st.markdown(
        """
        Parâmetros que podem ser configurados globalmente:
        - Dias padrão para aviso de aniversariantes
        - Configurações de integração com banco ou APIs externas
        - Modelos de e-mails e mensagens
        - Moeda, formato de datas, etc.
        """
    )

    # exemplo de parâmetro: dia do pagamento padrão
    dia_pagamento = st.number_input("Dia padrão de pagamento", min_value=1, max_value=31, value=5)
    st.info(f"Dia padrão de pagamento definido como {dia_pagamento} do mês.")

    # --------------------
    # Backup / Restauração de Base
    # --------------------
    st.subheader("💾 Backup / Restauração")
    st.markdown(
        """
        Aqui você poderia:
        - Baixar backup completo da base de dados
        - Restaurar a base a partir de um arquivo CSV ou Excel
        """
    )
    if st.button("Fazer backup da base de colaboradores"):
        df_backup = read_df()
        csv = df_backup.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Baixar backup CSV", csv, file_name="backup_colaboradores.csv", mime="text/csv")
        st.success("Backup gerado com sucesso!")

    # --------------------
    # Logs do Sistema
    # --------------------
    st.subheader("📜 Logs do Sistema")
    st.markdown(
        """
        - Aqui você poderia mostrar logs de acessos, alterações ou erros
        - Útil para auditoria ou depuração
        """
    )

# =========================================================
# Bloco 7 - RELATÓRIOS AVANÇADOS / DASHBOARDS
# =========================================================
elif pagina == "Relatórios Avançados":
    st.title("📊 Relatórios Avançados")

    # --------------------
    # Filtros principais
    # --------------------
    unidade_filtro = st.multiselect("Filtrar por Unidade", UNIDADES, default=UNIDADES)
    mes_inicio = st.date_input("Mês inicial", value=date.today().replace(day=1))
    mes_fim = st.date_input("Mês final", value=date.today().replace(day=1))

    # Normalizar para primeiro dia do mês
    mes_inicio_ref = date(mes_inicio.year, mes_inicio.month, 1)
    mes_fim_ref = date(mes_fim.year, mes_fim.month, 1)

    # --------------------
    # Resumo da Folha
    # --------------------
    q = """
        SELECT f.colaborador_id, f.colaborador_nome, f.unidade, f.mes_referencia,
               f.salario_base_cents, f.valor_depositado_cents
        FROM folha_pagamento f
        JOIN colaboradores c ON c.id = f.colaborador_id
        WHERE f.mes_referencia BETWEEN %s AND %s
    """
    df_folha = pd.read_sql_query(q, conn, params=(mes_inicio_ref, mes_fim_ref))
    if unidade_filtro:
        df_folha = df_folha[df_folha["unidade"].isin(unidade_filtro)]

    if df_folha.empty:
        st.info("Nenhum registro encontrado para o período selecionado.")
    else:
        # converter cents para reais
        df_folha["salario_base"] = df_folha["salario_base_cents"] / 100
        df_folha["valor_depositado"] = df_folha["valor_depositado_cents"].fillna(0) / 100

        # --------------------
        # Totais por unidade
        # --------------------
        tot_unit = df_folha.groupby("unidade").agg(
            Total_Salario=("salario_base", "sum"),
            Total_Depositos=("valor_depositado", "sum"),
            Qtde_Funcionarios=("colaborador_id", "nunique")
        ).reset_index()
        st.subheader("💰 Totais por Unidade")
        st.dataframe(tot_unit)

        # --------------------
        # Gráfico comparativo
        # --------------------
        fig = px.bar(tot_unit, x="unidade", y=["Total_Salario", "Total_Depositos"],
                     title="Comparativo Salário Base vs Depósitos",
                     labels={"value":"R$","unidade":"Unidade"})
        st.plotly_chart(fig, use_container_width=True)

        # --------------------
        # Exportar CSV/XLSX
        # --------------------
        st.subheader("Exportar dados")
        df_export = df_folha[["colaborador_id","colaborador_nome","unidade","mes_referencia","salario_base","valor_depositado"]]
        df_export.rename(columns={
            "colaborador_id":"ID",
            "colaborador_nome":"Nome",
            "unidade":"Unidade",
            "mes_referencia":"Mês Referência",
            "salario_base":"Salário Base (R$)",
            "valor_depositado":"Valor Depositado (R$)"
        }, inplace=True)
        csv = df_export.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar CSV", csv, file_name="relatorio_folha.csv", mime="text/csv")

# =========================================================
# Bloco 8 - FUNÇÕES AUXILIARES / HELPERS
# =========================================================

import io

# Converter centavos para reais
def cents_to_real(c):
    if c is None:
        return 0.0
    return round(c / 100.0, 2)

# Converter reais para centavos
def real_to_cents(r):
    if r is None:
        return 0
    return int(round(float(r) * 100))

# Validar CPF (simples)
def validar_cpf(cpf):
    cpf = re.sub(r'\D','', str(cpf))
    if len(cpf) != 11:
        return False
    # Pode implementar validação de dígito verificador se quiser
    return True

# Validar telefone
def validar_telefone(tel):
    pattern = re.compile(r'^\(\d{2}\)\s?\d{4,5}-\d{4}$')
    return bool(pattern.match(str(tel)))

# Normalizar datas para display
def format_date(d):
    if d is None:
        return "-"
    if isinstance(d, str):
        try:
            d = datetime.strptime(d, "%Y-%m-%d")
        except:
            return d
    return d.strftime("%d/%m/%Y")

# Função para download em XLSX
def download_excel(df, filename="dados.xlsx"):
    towrite = io.BytesIO()
    with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Dados")
    towrite.seek(0)
    return towrite
