- [ ] Inspect crawler uca: confirm cause (link extraction/queue/PDF errors)
- [x] Update `src/ingestion/web/chaimae_uca.py` with: debug logging, URL normalization/filtering, better error logging for PDFs, session+UA

- [ ] Run crawler and verify MinIO outputs: raw/html/*, raw/pdfs/*, raw/logs/uca/*
- [ ] If debug shows near-zero meaningful <a href> on homepage: switch to JS-render (Playwright)

