from docx.shared import Mm
from docxtpl import DocxTemplate, InlineImage

img_path = "glass_onion.jpg"
doc = DocxTemplate("tpl.docx")
visual = InlineImage(doc, img_path, width=Mm(150))
context = {
    "company_name": "Alpha",
    "owner": "Miles Bron",
    "killer": "Stop the spoiler",
    "visual": visual,
}
doc.render(context)
doc.save("script.docx")
