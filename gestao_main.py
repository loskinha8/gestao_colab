import streamlit as st
import pandas as pd
import plotly.express as px
import re
from datetime import datetime, date, timedelta
import os
import psycopg2

# --- Configuração da página ---
st.set_page_config(page_title="Gestão de Colaboradores", page_icon="👟", layout="wide")

# --- Conexão com banco ---
DATABASE_URL = os.getenv("ConnectDB")

if DATABASE_URL:
    DATABASE_URL = DATABASE_URL.strip().strip('"').strip("'")

if not DATABASE_URL:
    st.error("Variável de ambiente 'ConnectDB' não encontrada. Configure a DATABASE_URL/ConnectDB e reinicie o app.")
    st.stop()

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# --- Criar tabela (caso não exista) ---
cursor.execute("""
CREATE TABLE IF NOT EXISTS colaboradores (
    id SERIAL PRIMARY KEY,
    nome TEXT,
    conta_deposito TEXT,
    nascimento DATE,
    cpf TEXT,
    rg_outro TEXT,
    orgao_emissor TEXT,
    emissao DATE,
    admissao DATE,
    saida DATE,
    ativo INTEGER,
    funcao TEXT,
    salario_cents INTEGER,
    estado_civil TEXT,
    escolaridade TEXT,
    nacionalidade TEXT,
    naturalidade TEXT,
    cep TEXT,
    bairro TEXT,
    endereco TEXT,
    telefone TEXT,
    unidade TEXT,
    observacoes TEXT
)
""")
conn.commit()

# --------------------------
# Funções utilitárias
# --------------------------
def to_date_or_none(s):
    try:
        if not s or str(s).strip() == "":
            return None
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except:
        return None


def safe_parse_date(s):
    try:
        if not s or str(s).strip() == "":
            return None
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except Exception:
        return None


def read_df(where_clause=None, params=None):
    q = "SELECT * FROM colaboradores"
    if where_clause:
        q += " WHERE " + where_clause
    # pandas will use the DBAPI connection
    df = pd.read_sql_query(q, conn, params=params or [])
    if df.empty:
        return df
    df["ativo"] = df["ativo"].fillna(0).astype(int)
    df["salario_cents"] = df["salario_cents"].fillna(0).astype(int)
    df["salario_reais"] = df["salario_cents"] / 100
    return df

# --------------------------
# Constantes
# --------------------------
UNIDADES = ["Serrinha", "Anguera", "Coração de Maria", "Ipirá"]
ESCOLARIDADES = ["E.M. Completo", "E.M. Incompleto", "E.F. Completo", "E.F. Incompleto", "Ensino Superior", "Sem escolaridade"]
ESTADOS_CIVIS = ["Solteiro(a)", "Casado(a)", "Viúvo(a)", "Divorciado(a)"]
FUNCOES = ["Alimentador de Linha de Produção", "Auxiliar Administrativo(a)"]

# --- Menu lateral ---
st.sidebar.title("📂 Navegação")
pagina = st.sidebar.radio("Ir para:", ["Gestão de Colaboradores", "Relatórios e Estatísticas"])

# =========================================================
# GESTÃO
# =========================================================
if pagina == "Gestão de Colaboradores":
    st.title("👟 Gestão de Colaboradores")
    aba = st.radio("Escolha uma ação:", ["➕ Adicionar", "✏️ Editar", "🗑️ Excluir"], horizontal=True)

    # -------------------------
    # ADICIONAR
    # -------------------------
    if aba == "➕ Adicionar":
        st.subheader("Adicionar novo colaborador")
        with st.form("novo_colab"):
            col1, col2, col3 = st.columns(3)
            with col1:
                nome = st.text_input("Nome", placeholder="Nome completo do colaborador")
            with col2:
                funcao = st.selectbox("Função", FUNCOES, index=None)
            with col3:
                unidade = st.selectbox("Unidade", UNIDADES, index=None)

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                salario = st.number_input("Salário (R$)", min_value=0.0, step=0.01, value=0.0)
            with col2:
                ativo_str = st.selectbox("Status", ["Ativo", "Não-ativo"]) 
                ativo = True if ativo_str == "Ativo" else False
            with col3:
                admissao = st.date_input("Admissão", value=None)
            with col4:
                saida = st.date_input("Saída", value=None)

            col1, col2, col3 = st.columns(3)
            with col1:
                cpf = st.text_input("CPF", max_chars=14, placeholder="000.000.000-00")
            with col2:
                rg_outro = st.text_input("RG/Outro", placeholder="Número do RG ou outro documento")
            with col3:
                orgao_emissor = st.text_input("Órgão Emissor", placeholder="Ex.: SSP/BA")

            col1, col2 = st.columns(2)
            with col1:
                emissao = st.date_input("Emissão", value=None)
            with col2:
                nascimento = st.date_input("Nascimento", value=None)

            col1, col2, col3 = st.columns(3)
            with col1:
                estado_civil = st.selectbox("Estado Civil", ESTADOS_CIVIS, index=None)
            with col2:
                escolaridade = st.selectbox("Escolaridade", ESCOLARIDADES, index=None)
            with col3:
                nacionalidade = st.selectbox("Nacionalidade", ["Brasileiro(a)"], index=None)

            col1, col2 = st.columns(2)
            with col1:
                naturalidade = st.text_input("Naturalidade", placeholder="Cidade/UF")
            with col2:
                conta_deposito = st.text_input("Conta de Depósito", placeholder="Chave pix ou banco p/ depósito")

            col1, col2, col3 = st.columns(3)
            with col1:
                cep = st.text_input("CEP", max_chars=9, placeholder="00000-000")
            with col2:
                bairro = st.text_input("Bairro", placeholder="Ex.: Centro")
            with col3:
                endereco = st.text_input("Endereço", placeholder="Rua, complemento, número")

            telefone = st.text_input("Telefone", placeholder="(00) 00000-0000", max_chars=15)
            observacoes = st.text_area("Observações", placeholder="Anotações adicionais sobre o colaborador")

            submitted = st.form_submit_button("Salvar")

            if submitted:
                if not nome.strip():
                    st.error("⚠️ O campo 'Nome' é obrigatório.")
                else:
                    salario_cents = int(round(float(salario or 0.0) * 100))
                    ativo_val = 1 if ativo else 0
                    admissao_v = safe_parse_date(admissao)
                    saida_v = safe_parse_date(saida)
                    emissao_v = safe_parse_date(emissao)
                    nascimento_v = safe_parse_date(nascimento)

                    cursor.execute("""
                        INSERT INTO colaboradores (
                            nome, conta_deposito, nascimento, cpf, rg_outro, orgao_emissor,
                            emissao, admissao, saida, ativo, funcao, salario_cents,
                            estado_civil, escolaridade, nacionalidade, naturalidade,
                            cep, bairro, endereco, telefone, unidade, observacoes
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        nome, conta_deposito, nascimento_v, cpf, rg_outro, orgao_emissor,
                        emissao_v, admissao_v, saida_v, ativo_val, funcao, salario_cents,
                        estado_civil, escolaridade, nacionalidade, naturalidade,
                        cep, bairro, endereco, telefone, unidade, observacoes
                    ))
                    conn.commit()
                    st.success(f"✅ Colaborador {nome} adicionado com sucesso!")

    # -------------------------
    # EDITAR
    # -------------------------
    elif aba == "✏️ Editar":
        st.subheader("Editar colaborador existente")
        df_ids = pd.read_sql_query("SELECT id, nome FROM colaboradores ORDER BY nome", conn)
        if df_ids.empty:
            st.info("Nenhum colaborador cadastrado ainda.")
        else:
            colab_id = st.selectbox("Selecione o colaborador", df_ids["id"],
                                    format_func=lambda x: df_ids.loc[df_ids["id"] == x, "nome"].values[0])
            dados = pd.read_sql_query("SELECT * FROM colaboradores WHERE id = %s", conn, params=(colab_id,)).iloc[0]

            with st.form("editar_colab"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    nome = st.text_input("Nome", dados["nome"] or "")
                with col2:
                    funcao_atual = dados["funcao"]
                    idx_func = FUNCOES.index(funcao_atual) if funcao_atual in FUNCOES else 0
                    funcao = st.selectbox("Função", FUNCOES, index=idx_func)
                with col3:
                    unidade_atual = dados["unidade"]
                    idx_uni = UNIDADES.index(unidade_atual) if unidade_atual in UNIDADES else 0
                    unidade = st.selectbox("Unidade", UNIDADES, index=idx_uni)

                col1, col2, col3, col4 = st.columns(4)
                default_sal = (int(dados["salario_cents"]) if pd.notna(dados["salario_cents"]) else 0) / 100.0
                with col1:
                    salario = st.number_input("Salário (R$)", min_value=0.0, step=0.01, value=default_sal)
                with col2:
                    ativo_str = st.selectbox("Status", ["Ativo", "Não-ativo"], index=0 if int(dados["ativo"]) == 1 else 1)
                    ativo = True if ativo_str == "Ativo" else False
                with col3:
                    admissao = st.date_input("Admissão", to_date_or_none(dados["admissao"]))
                with col4:
                    saida = st.date_input("Saída", to_date_or_none(dados["saida"]))

                col1, col2, col3 = st.columns(3)
                with col1:
                    cpf = st.text_input("CPF", dados["cpf"] or "", placeholder="000.000.000-00", max_chars=14)
                with col2:
                    rg_outro = st.text_input("RG/Outro", dados["rg_outro"] or "", placeholder="Número do RG ou outro documento")
                with col3:
                    orgao_emissor = st.text_input("Órgão Emissor", dados["orgao_emissor"] or "", placeholder="Ex.: SSP/BA")

                col1, col2 = st.columns(2)
                with col1:
                    emissao = st.date_input("Emissão", to_date_or_none(dados["emissao"]))
                with col2:
                    nascimento = st.date_input("Nascimento", to_date_or_none(dados["nascimento"]))

                col1, col2, col3 = st.columns(3)
                with col1:
                    estado_civil_atual = dados["estado_civil"]
                    idx_ec = ESTADOS_CIVIS.index(estado_civil_atual) if estado_civil_atual in ESTADOS_CIVIS else 0
                    estado_civil = st.selectbox("Estado Civil", ESTADOS_CIVIS, index=idx_ec)
                with col2:
                    esc_atual = dados["escolaridade"]
                    idx_esc = ESCOLARIDADES.index(esc_atual) if esc_atual in ESCOLARIDADES else 0
                    escolaridade = st.selectbox("Escolaridade", ESCOLARIDADES, index=idx_esc)
                with col3:
                    NACIONALIDADES = ["Brasileiro(a)"]
                    nac_atual = dados["nacionalidade"]
                    idx_nac = NACIONALIDADES.index(nac_atual) if nac_atual in NACIONALIDADES else 0
                    nacionalidade = st.selectbox("Nacionalidade", NACIONALIDADES, index=idx_nac)

                col1, col2 = st.columns(2)
                with col1:
                    naturalidade = st.text_input("Naturalidade", dados["naturalidade"] or "", placeholder="Cidade/UF")
                with col2:
                    conta_deposito = st.text_input("Conta de Depósito", dados["conta_deposito"] or "", placeholder="Chave pix ou banco p/ depósito")

                col1, col2, col3 = st.columns(3)
                with col1:
                    cep = st.text_input("CEP", dados["cep"] or "", placeholder="00000-000", max_chars=9)
                with col2:
                    bairro = st.text_input("Bairro", dados["bairro"] or "", placeholder="Ex.: Centro")
                with col3:
                    endereco = st.text_input("Endereço", dados["endereco"] or "", placeholder="Rua, complemento, número")

                telefone = st.text_input("Telefone", dados["telefone"] or "", placeholder="(00) 00000-0000", max_chars=15)
                observacoes = st.text_area("Observações", dados["observacoes"] or "", placeholder="Anotações adicionais sobre o colaborador")

                submitted = st.form_submit_button("Salvar alterações")

                if submitted:
                    salario_cents = int(round(float(salario or 0.0) * 100))
                    ativo_val = 1 if ativo else 0
                    admissao_v = safe_parse_date(admissao)
                    saida_v = safe_parse_date(saida)
                    emissao_v = safe_parse_date(emissao)
                    nascimento_v = safe_parse_date(nascimento)

                    cursor.execute("""
                        UPDATE colaboradores
                        SET nome=%s, conta_deposito=%s, nascimento=%s, cpf=%s, rg_outro=%s, orgao_emissor=%s,
                            emissao=%s, admissao=%s, saida=%s, ativo=%s, funcao=%s, salario_cents=%s,
                            estado_civil=%s, escolaridade=%s, nacionalidade=%s, naturalidade=%s,
                            cep=%s, bairro=%s, endereco=%s, telefone=%s, unidade=%s, observacoes=%s
                        WHERE id=%s
                    """, (
                        nome, conta_deposito, nascimento_v, cpf, rg_outro, orgao_emissor,
                        emissao_v, admissao_v, saida_v, ativo_val, funcao, salario_cents,
                        estado_civil, escolaridade, nacionalidade, naturalidade,
                        cep, bairro, endereco, telefone, unidade, observacoes, colab_id
                    ))
                    conn.commit()
                    st.success("✅ Alterações salvas com sucesso!")

    # -------------------------
    # EXCLUIR
    # -------------------------
    elif aba == "🗑️ Excluir":
        st.subheader("Excluir colaborador")
        df_ids = pd.read_sql_query("SELECT id, nome FROM colaboradores ORDER BY nome", conn)
        if df_ids.empty:
            st.info("Nenhum colaborador cadastrado ainda.")
        else:
            colab_id = st.selectbox("Selecione o colaborador para excluir", df_ids["id"],
                                    format_func=lambda x: df_ids.loc[df_ids["id"] == x, "nome"].values[0])
            nome_colab = df_ids.loc[df_ids["id"] == colab_id, "nome"].values[0]
            if st.button(f"🗑️ Confirmar exclusão de {nome_colab}"):
                cursor.execute("DELETE FROM colaboradores WHERE id = %s", (colab_id,))
                conn.commit()
                st.warning(f"Colaborador {nome_colab} foi removido permanentemente.")

    # -------------------------
    # VISUALIZAR
    # -------------------------
    st.markdown("---")
    st.subheader("📋 Lista de colaboradores")
    filtro_unidade = st.multiselect("Filtrar por unidade", UNIDADES)
    filtro_ativo = st.selectbox("Status", ["Todos", "Ativos", "Inativos"])

    where_clauses = []
    params = []
    if filtro_unidade:
        placeholders = ','.join(['%s']*len(filtro_unidade))
        where_clauses.append(f"unidade IN ({placeholders})")
        params.extend(filtro_unidade)
    if filtro_ativo == "Ativos":
        where_clauses.append("ativo = 1")
    elif filtro_ativo == "Inativos":
        where_clauses.append("ativo = 0")

    where = " AND ".join(where_clauses) if where_clauses else None
    df_vis = read_df(where, params)
    if not df_vis.empty:
        df_vis["ativo_texto"] = df_vis["ativo"].map({1: "Ativo", 0: "Não-ativo"})
        cols = df_vis.columns.tolist()
        default_cols = ["id", "nome", "funcao", "unidade", "salario_reais", "ativo_texto"]
        selected = st.multiselect("Colunas para exibir", cols, default=[c for c in default_cols if c in cols])
        st.dataframe(df_vis[selected])
    else:
        st.info("Nenhum colaborador encontrado com esses filtros.")

# =========================================================
# RELATÓRIOS E ESTATÍSTICAS
# =========================================================
elif pagina == "Relatórios e Estatísticas":
    st.title("📊 Relatórios e Estatísticas")
    df = read_df()
    if df.empty:
        st.info("Nenhum dado cadastrado ainda.")
    else:
        # --- Filtros ---
        st.sidebar.markdown("### Filtros (Relatórios)")
        sel_unidades = st.sidebar.multiselect("Unidades", options=sorted(df["unidade"].dropna().unique()), default=sorted(df["unidade"].dropna().unique()))
        sel_status = st.sidebar.multiselect("Status", options=["Ativos", "Inativos"], default=["Ativos", "Inativos"])

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

# Fim do arquivo
