import data_scraping.datautils as du
from datetime import datetime

###Information-to-disk methods
def error_processor(calling_func_name, errors):
    'Write errors to disk'
    print "error processor fired by " + calling_func_name
    print "errors are: "
    print errors
    du.write_log(calling_func_name + "_errors_" + str(datetime.now()).replace(" ", "_"), errors)
    return

def remaining_query_dump(single_repos, resetAt, since, multi_repos=[], idx=0, single_repos_only=False):
    'If query exceeds limit, write remaining repos to search to disk'
    output = {}
    single_repos = single_repos.ix[idx:len(single_repos)]
    single_repos = single_repos[["owner", "main_repo"]].set_index("owner")
    single_remaining_repos = single_repos.to_dict()
    single_remaining_repos = single_remaining_repos["main_repo"]
    if single_repos_only == True:
        output["single_repo_orgs"] = single_remaining_repos
    else:
        output["single_repo_orgs"] = single_remaining_repos
        output.update["multi_repo_orgs"] = multi_repos
    now = du.get_time(now =True, utc_string=True)
    output["date"] = now
    output["resetAt"] = resetAt
    output["since"] = since
    du.write_json("remaining_repos_"+now, output)