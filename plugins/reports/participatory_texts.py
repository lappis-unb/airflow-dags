from datetime import datetime


class DataFilter:
    """Filtro dos dados extraidos de consulta pública."""

    def __init__(self, data):
        self.data = data

    def filter_data(self):
        filtered_data = []
        seen_titles = set()

        for item in self.data:
            proposal_title = item["proposal_title"]
            comments = item["comments"]

            if proposal_title not in seen_titles:
                seen_titles.add(proposal_title)
                comments_list = []

                for comment in comments:
                    created_at = datetime.strptime(comment["createdAt"], "%Y-%m-%dT%H:%M:%S%z")
                    formatted_created_at = created_at.strftime("%d/%m/%Y %H:%M")
                    comment_body = comment["body"]
                    author_name = comment["author_name"]
                    formatted_comment = f"{comment_body} - {author_name}, {formatted_created_at}"
                    comments_list.append(formatted_comment)

                proposal_data = {"proposal_title": proposal_title, "comments": comments_list}
                filtered_data.append(proposal_data)
            else:
                for proposal in filtered_data:
                    if proposal["proposal_title"] == proposal_title:
                        for comment in comments:
                            created_at = datetime.strptime(comment["createdAt"], "%Y-%m-%dT%H:%M:%S%z")
                            formatted_created_at = created_at.strftime("%d/%m/%Y %H:%M")
                            comment_body = comment["body"]
                            author_name = comment["author_name"]
                            formatted_comment = f"{comment_body} - {author_name}, {formatted_created_at}"
                            proposal["comments"].append(formatted_comment)
                        break

        return filtered_data
