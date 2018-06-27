import data_scraping.gitservice.db_interface as devdb


devdb.get_github_analytics_data(stars=True, issues=True, forks=True,pullrequests=True,commits=True,stats=True, orgtotals=True)
