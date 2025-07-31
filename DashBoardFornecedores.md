import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from datetime import datetime   
import pytz
import pyodbc
from threading import Lock
import socket

# ==============================================
# CONFIGURAÇÕES DO BANCO DE DADOS
# ==============================================
DB_CONFIG = {
    'server': '192.168.4.219',
    'database': 'DataControl',
    'username': 'fecsantos',
    'password': 'dYAvWcBZe2GspzQZBIg1',
    'driver': 'ODBC Driver 17 for SQL Server'
}

CONN_STR = f"DRIVER={{{DB_CONFIG['driver']}}};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"

# ==============================================
# FUNÇÕES AUXILIARES
# ==============================================
def get_local_ip():
    """Obtém o IP local automaticamente"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

data_lock = Lock()
cached_data = None
last_update = None

def fetch_data():
    global cached_data, last_update
    
    with data_lock:
        try:
            conn = pyodbc.connect(CONN_STR)
            cursor = conn.cursor()
            
            query = """
                SELECT Fornecedor, Tabela, Data, Gap_Minutos, Status 
                FROM LogFornecedoresDadosTransacionais
                ORDER BY Data DESC
            """
            
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            
            df = pd.DataFrame.from_records(data, columns=columns)
            df['Data'] = pd.to_datetime(df['Data'])
            
            conn.close()
            
            cached_data = df
            last_update = datetime.now(pytz.timezone('America/Sao_Paulo'))
            return df
            
        except Exception as e:
            print(f"ERRO NO BANCO DE DADOS: {str(e)}")
            return cached_data if cached_data is not None else pd.DataFrame()

# ==============================================
# INICIALIZAÇÃO DO APP DASH
# ==============================================
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{
        'name': 'viewport',
        'content': 'width=device-width, initial-scale=1.0'
    }]
)
server = app.server

# ==============================================
# LAYOUT DO DASHBOARD
# ==============================================
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("📊 Monitoramento de Gaps - Recebimento de Arquivos", 
                   className="text-center my-3"),
            html.Div(id='refresh-time', className="text-end mb-2 text-muted")
        ], width=12)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col(dbc.Card(id='card-aspect', className="shadow-sm"), width=3),
        dbc.Col(dbc.Card(id='card-olos', className="shadow-sm"), width=3),
        dbc.Col(dbc.Card(id='card-responsys', className="shadow-sm"), width=3),
        dbc.Col(dbc.Card(id='card-robbu', className="shadow-sm"), width=3)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("🔍 Status por Fornecedor", className="fw-bold"),
                dbc.CardBody(
                    dcc.Graph(
                        id='fornecedor-summary',
                        style={'height': '400px'}
                    )
                )
            ], className="shadow-sm")
        ], md=6),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("📋 Detalhes por Tabela", className="fw-bold"),
                dbc.CardBody([
                    dcc.Dropdown(
                        id='fornecedor-dropdown',
                        options=[],
                        value=None,
                        placeholder="Selecione um fornecedor...",
                        className="mb-3"
                    ),
                    dcc.Graph(
                        id='tabela-details',
                        style={'height': '350px'}
                    )
                ])
            ], className="shadow-sm")
        ], md=6)
    ], className="mb-4"),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("⏳ Linha do Tempo de Recebimentos", className="fw-bold"),
                dbc.CardBody(
                    dcc.Graph(
                        id='timeline-chart',
                        style={'height': '400px'}
                    )
                )
            ], className="shadow-sm")
        ], md=8),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("🚨 Últimos Alertas", className="fw-bold"),
                dbc.CardBody(
                    html.Div(id='alert-table', style={
                        'height': '370px', 
                        'overflowY': 'auto',
                        'padding': '5px'
                    })
                )
            ], className="shadow-sm")
        ], md=4)
    ], className="mb-4"),
    
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,
        n_intervals=0
    ),
    
    html.Footer([
        html.P("© 2024 Sistema de Monitoramento - DataControl", 
              className="text-center text-muted small mt-4")
    ])
], fluid=True, className="py-3", style={'maxHeight': '100vh', 'overflowY': 'auto'})

# ==============================================
# CALLBACKS
# ==============================================
@app.callback(
    [Output('fornecedor-dropdown', 'options'),
     Output('fornecedor-dropdown', 'value'),
     Output('refresh-time', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dropdowns(n):
    df = fetch_data()
    fornecedores = [{'label': f, 'value': f} for f in df['Fornecedor'].unique()]
    ultima_atualizacao = last_update.strftime("%d/%m/%Y %H:%M:%S") if last_update else "Nunca"
    
    return (
        fornecedores,
        fornecedores[0]['value'] if fornecedores else None,
        f"🔄 Última atualização: {ultima_atualizacao}"
    )

@app.callback(
    [Output('card-aspect', 'children'),
     Output('card-olos', 'children'),
     Output('card-responsys', 'children'),
     Output('card-robbu', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_cards(n):
    df = fetch_data()
    cards = []
    
    for fornecedor in ['ASPECT', 'OLOS', 'RESPONSYS', 'ROBBU']:
        subset = df[df['Fornecedor'] == fornecedor]
        total = len(subset)
        problemas = len(subset[subset['Status'] != 'OK'])
        
        card_content = [
            dbc.CardHeader(fornecedor, className="fw-bold"),
            dbc.CardBody([
                html.H4(f"{problemas}/{total}", className="card-title text-center"),
                html.P("tabelas com alerta" if problemas != 1 else "tabela com alerta", 
                      className="card-text text-center")
            ])
        ]
        
        color = "danger" if problemas > 0 else "success"
        cards.append(dbc.Card(card_content, color=color, inverse=True))
    
    return cards

@app.callback(
    Output('fornecedor-summary', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_summary(n):
    df = fetch_data()
    
    if df.empty:
        return px.bar(title="Sem dados disponíveis")
    
    summary = df.groupby(['Fornecedor', 'Status']).size().reset_index(name='Count')
    
    fig = px.bar(summary, x='Fornecedor', y='Count', color='Status',
                 color_discrete_map={
                     'OK': '#28a745', 
                     'ATENÇÃO: GAP MAIOR QUE 30 MIN': '#dc3545'
                 },
                 title="",
                 labels={'Count': 'Quantidade de Tabelas'})
    
    fig.update_layout(
        barmode='stack', 
        xaxis_title="",
        yaxis_title="",
        hovermode="x unified",
        height=350,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig

@app.callback(
    Output('tabela-details', 'figure'),
    [Input('fornecedor-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_tabela_details(fornecedor, n):
    df = fetch_data()
    
    if not fornecedor or df.empty:
        return px.bar(title="Selecione um fornecedor")
    
    filtered_df = df[df['Fornecedor'] == fornecedor]
    
    fig = px.bar(filtered_df, x='Tabela', y='Gap_Minutos', color='Status',
                 color_discrete_map={
                     'OK': '#28a745', 
                     'ATENÇÃO: GAP MAIOR QUE 30 MIN': '#dc3545'
                 },
                 title="",
                 hover_data=['Data'],
                 labels={'Gap_Minutos': 'Minutos de atraso'})
    
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Minutos de Atraso",
        xaxis={'tickangle': 45},
        height=300,
        margin=dict(l=20, r=20, t=20, b=100),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig

@app.callback(
    Output('timeline-chart', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_timeline(n):
    df = fetch_data()
    
    if df.empty:
        return px.scatter(title="Sem dados disponíveis")
    
    fig = px.scatter(df, x='Data', y='Fornecedor', color='Status',
                     color_discrete_map={
                         'OK': '#28a745', 
                         'ATENÇÃO: GAP MAIOR QUE 30 MIN': '#dc3545'
                     },
                     hover_data=['Tabela', 'Gap_Minutos'],
                     title="",
                     labels={'Data': 'Data/Hora', 'Fornecedor': ''})
    
    fig.update_traces(
        marker=dict(size=12, opacity=0.7, line=dict(width=1, color='DarkSlateGrey')))
    fig.update_layout(
        yaxis_title="",
        xaxis_title="Data/Hora",
        hovermode="closest",
        height=350,
        margin=dict(l=20, r=20, t=20, b=20),
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    return fig

@app.callback(
    Output('alert-table', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_alert_table(n):
    df = fetch_data()
    
    if df.empty:
        return html.P("Nenhum dado disponível", className="text-muted text-center")
    
    alertas = df[df['Status'] != 'OK'].sort_values('Data', ascending=False)
    
    if alertas.empty:
        return html.Div([
            html.I(className="bi bi-check-circle-fill text-success me-2"),
            "✅ Todas as tabelas estão atualizadas"
        ], className="text-center p-3")
    
    return dbc.Table([
        html.Thead(
            html.Tr([
                html.Th("Fornecedor"),
                html.Th("Tabela"),
                html.Th("Atraso"),
                html.Th("Data")
            ], className="table-primary")
        ),
        html.Tbody([
            html.Tr([
                html.Td(row['Fornecedor']),
                html.Td(row['Tabela']),
                html.Td(f"{row['Gap_Minutos']} min", className="text-danger fw-bold"),
                html.Td(row['Data'].strftime("%d/%m %H:%M"))
            ], className="table-light") for _, row in alertas.head(10).iterrows()
        ])
    ], striped=True, hover=True, responsive=True, className="mt-2")

# ==============================================
# INICIALIZAÇÃO
# ==============================================
if __name__ == '__main__':
    # Configurações para rede
    PORT = 8050
    HOST = '0.0.0.0'
    local_ip = get_local_ip()
    
    print(f"\n⏳ Iniciando aplicação...")
    print(f"🔗 Acesso LOCAL: http://localhost:{PORT}")
    print(f"🌐 Acesso na REDE: http://{local_ip}:{PORT}\n")
    
    # Primeira carga de dados
    fetch_data()
    
    # Inicia o servidor
    app.run(host=HOST, port=PORT, debug=False)
