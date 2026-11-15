# data/

**Owner:** shared. Everything in this folder except this README is gitignored.

Expected layout (create the folders as needed):

```
data/
├── statsbomb/
│   └── events/<match_id>.json      # StatsBomb Open Data: one events file per match
└── kaggle/
    └── database.sqlite             # Kaggle "European Soccer Database"
```

- StatsBomb Open Data: https://github.com/statsbomb/open-data (files `data/events/<match_id>.json`)
- If a match file is missing, the replay producer can download it with `statsbombpy`.
