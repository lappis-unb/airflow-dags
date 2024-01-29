import pandas as pd
import matplotlib.pyplot as plt
import re
from io import BytesIO
import base64
import geopandas as gpd
from docx import Document
from docx2pdf import convert


class ReportGenerator:
    def __init__(self):
        pass

    def filter_proposals(self, data, states, pattern):
        proposals = [proposal for proposal in data["data"]["component"]["proposals"]["nodes"] 
                     if proposal["state"] not in states and bool(re.match(pattern, proposal['title']['translation']))]
        return proposals

    def create_bp_dataframe(self, proposals):
        df_bp = pd.DataFrame(proposals)
        df_bp['publishedAt'] = pd.to_datetime(df_bp['publishedAt'])
        df_bp['updatedAt'] = pd.to_datetime(df_bp['updatedAt'])
        df_bp['translation'] = df_bp['title'].apply(lambda x: x['translation'])
        return df_bp

    def calculate_totals(self, data):
        num_proposals = len(data)
        num_votes = data['voteCount'].sum()
        num_comments = data['totalCommentsCount'].sum()
        return num_proposals, num_votes, num_comments

    def generate_daily_plot(self, df_bp):
        df_bp['Data'] = df_bp['publishedAt'].dt.date

        n_proposals = df_bp.groupby('Data')['id'].count()
        n_comments = df_bp.groupby('Data')['totalCommentsCount'].sum()
        n_votes = df_bp.groupby('Data')['voteCount'].sum()

        plt.figure(figsize=(12, 6))
        plt.plot(n_proposals.index, n_proposals.values, label='Propostas', color='blue', marker='o')
        plt.plot(n_comments.index, n_comments.values, label='Comentários', color='green', marker='s')
        plt.plot(n_votes.index, n_votes.values, label='Votos', color='red', marker='^')

        plt.xlabel('Data')
        plt.ylabel('Quantidade')
        plt.title('Quantidade de Propostas, Comentários e Votos por Dia')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        daily_graph = base64.b64encode(buffer.getvalue()).decode('utf-8')

        buffer.close()

        return daily_graph
    
    def load_data(self, shp_path, json_path):
        brasil = gpd.read_file(shp_path)
        dados = pd.read_json(json_path)
        return brasil, dados

    def filter_and_rename(self, dados, pais, coluna):
        dados_filtrados = dados[dados['country'] == pais]
        dados_filtrados = dados_filtrados.rename(columns={'region': coluna})
        return dados_filtrados

    def create_map(self, brasil, dados, index_coluna, join_coluna):
        mapa = brasil.set_index(index_coluna).join(dados.set_index(join_coluna))
        return mapa

    def plot_map(self, mapa, coluna):
        fig, ax = plt.subplots(figsize=(12, 8))
        mapa.boundary.plot(ax=ax, linewidth=0.5, color='k')
        mapa.plot(column=coluna, ax=ax, legend=True, cmap='YlOrRd')
        plt.title("Visitas por Estado no Brasil")
        plt.axis('off')
        plt.show()

        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        map_graph = base64.b64encode(buffer.getvalue()).decode('utf-8')

        buffer.close()

        return map_graph
    
    def insert_data_docx(template_path, data_to_insert):
        doc = Document(template_path)

        doc.add_paragraph(data_to_insert)

        doc.save('report.docx')
        convert('report.docx')
        
        convert("report.docx", "report.pdf")
        