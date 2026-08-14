# Data preservation CRITICALITY
reroll-data contains the very important data/v.db sqlite database. This database contains important data about the whole pypi ecosystem, as well as our tooling's attempt to interact with those wheels. You must treat this database with upmost respect and NEVER delete the database or a table.
You MUST NEVER create junk rows or temporary tables or columns. If you would like to make a schema change you MUST confirm it with the user and the schema change MUST be non-destructive.

Since the database is so large you should resist table-scanning operations when developing new features. Instead, sample rows, create the code based on that sample, and then let the user run the commands against the entire database

# Notebooks for exploration
Nteract notebooks (stored in the notebooks/ folder) are the preferred way to explore data. There are two-fold benefits to using nteract. First, during data exploration, any dataframe you pull into the notebook will persist, so you can make an expensive sql query once, and then afterwards manipulate the data in python. Second, the notebooks are durable artifacts. The notebooks in their final state are intended to be project reports - walking through any interested party through a narrative. Notebooks should follow this structure
```
Summary markdown

Data setup, copying from sql, and cleaning

Aggregation and analysis

Reports, and secondary analysis from the primary findings

Conclusion and Recommendations
```

Do NOT reach for the `sqlite3` CLI, `bash sqlite3 ...`, or ad-hoc `python3 -c` one-liners against `data/v.db` -- not even for a quick "let me just check something" query. This includes exploratory/scratch queries you don't intend to keep. Every query against v.db, no matter how small or throwaway, must run as a cell in the active nteract notebook, so the exploration trail is preserved and reusable. If there is no notebook open yet for the task, create one (or connect to the one already open) before running any SQL.

You should keep this format in mind, even when doing exploratory work. Prefer to create a proper setup & cleaning cell once you know the structure of your data & goals, as opposed to creating ever-more-cells. Once a proper finding is uncovered, or data is adapted in the proper way, prefer editing existing cells, even previous cells to support the needed direction of the notebook. Save the beginning and end summary sections until after the notebook analysis is complete, so it will always match your final conclusions and findings.

When making a new notebook, you must set the package manager to be uv, otherwise nteract will incorrectly try to reuse the pyproject.toml
