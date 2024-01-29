import pandas as pd
import matplotlib.pyplot as plt
import re
from io import BytesIO
import base64

class ReportGenerator:
    def __init__(self):
        pass

    def filter_proposals(self, data, states, pattern):
        proposals = [proposal for proposal in data["data"]["component"]["proposals"]["nodes"] 
                     if proposal["state"] not in states and bool(re.match(pattern, proposal['title']['translation']))]
        return proposals

    def create_dataframe(self, proposals):
        df = pd.DataFrame(proposals)
        df['publishedAt'] = pd.to_datetime(df['publishedAt'])
        df['updatedAt'] = pd.to_datetime(df['updatedAt'])
        df['translation'] = df['title'].apply(lambda x: x['translation'])
        return df

    def calculate_totals(self, data):
        num_proposals = len(data)
        num_votes = data['voteCount'].sum()
        num_comments = data['totalCommentsCount'].sum()
        return num_proposals, num_votes, num_comments

    def generate_daily_plot(self, df):
        df['Data'] = df['publishedAt'].dt.date

        n_proposals = df.groupby('Data')['id'].count()
        n_comments = df.groupby('Data')['totalCommentsCount'].sum()
        n_votes = df.groupby('Data')['voteCount'].sum()

        plt.figure(figsize=(12, 6))
        plt.plot(n_proposals.index, n_proposals.values, label='Propostas', color='blue', marker='o')
        plt.plot(n_comments.index, n_comments.values, label='Comentários', color='green', marker='s')
        plt.plot(n_votes.index, n_votes.values, label='Votos', color='red', marker='^')

        plt.xlabel('Data')
        plt.ylabel('Quantidade')
        plt.title('Quantidade de Propostas, Comentários e Votos por Dia (Geral)')
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)

        encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')

        buffer.close()

        return encoded_image
