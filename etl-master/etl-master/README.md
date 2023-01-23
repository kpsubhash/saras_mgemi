Getting Started:

1)  Install dbt on your local machine → dbt docs: https://docs.getdbt.com/

2) Clone the dbt repo locally → https://github.com/mgemi/etl

3) dbt's profiles.yml is in folder .dbt. You should not need to touch this unless you want to create your own personal dev schema (dbt_$your_name)

4) To make changes, make a branch, follow normal git workflow

5) Build models, make edits, query data. Run "dbt run --full-refresh --target dev" to fully drop and rebuild the data model NOTE: this can take a while depending on how big the dbt models are. 
Other commands you might use: "dbt run --target dev", "dbt seed --full-refresh --target dev", "dbt test --target dev", "dbt docs serve --port $desired_port" (By default the "Target" is dev so you shouldnt need to include the "--target dev" flag, but I usually do it anyways.)

5) Pull Request
https://mgemieng.atlassian.net/wiki/spaces/DS/pages/765591579/M.Gemi+Data+Model+Flow

To manually Update the data:
dbt run --target prod

To manually Update the data MODEL and the data [takes longer]:
dbt run --full-refresh --target prod

To manually run the test suite:
dbt test --target prod
