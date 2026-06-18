import pypdf

reader = pypdf.PdfReader('week07_机器学习模型流式部署与实时打标.pdf')
with open('week07.txt', 'w', encoding='utf-8') as f:
    for page in reader.pages:
        f.write(page.extract_text() + '\n')
