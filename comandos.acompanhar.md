# Ver as últimas linhas do log (atualiza a cada 5s)
tail -f classificar_gpt.log

# Ver quantos registros já foram processados
wc -l data/resultado/2026-03-14/pncp_filtrado_gpt.progresso

# Ver quantos relevantes já foram salvos no banco
sqlite3 data/resultado/2026-03-14/pncp_filtrado_gpt.db \
  "SELECT COUNT(*) FROM contratacoes_filtradas;"

# Ver os últimos relevantes encontrados
sqlite3 data/resultado/2026-03-14/pncp_filtrado_gpt.db \
  "SELECT pncp_id, categoria, substr(objeto_compra,1,80) FROM contratacoes_filtradas ORDER BY id DESC LIMIT 5;"

  # 5 workers paralelos com Gemini
python main.py --etapa classificar_gpt --data 2026-03-14 \
               --provedor gemini --workers 5

# modo sequencial (padrão, igual ao antes)
python main.py --etapa classificar_gpt --data 2026-03-14 \
               --provedor gemini