import analysis.pre_processing.dev_pre_processing as dpp

commit_data = dpp.pre_process_stats(commits=True)
commits_6mo = dpp.prune_data(commit_data, stat="commits", period="6mo",exclude_zeros=True)
