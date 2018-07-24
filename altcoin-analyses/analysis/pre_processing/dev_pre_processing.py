import data_scraping.gitservice.db_interface as dbi
import custom_utils.datautils as du
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from scipy.stats import expon

def regress_stat(y, corr_coef_only=False):
    if corr_coef_only == True:
        return stats.linregress(range(0, len(y) - 1), list(y[0:len(y)-1]))[2]
    else:
        return stats.linregress(range(0, len(y) - 1), list(y[0:len(y)-1]))

def pre_process_stats(commits=False, committers=False, stars=False, forks=False, issues=False, pullrequests=False, format_="stats", timescale="month"):
    results = {"aggregate_statistics":{},"org_statistics":{}}
    data_list = ['commits', 'committers', 'stars', 'forks', 'issues', 'pullrequests']
    data_list = du.select_elements_by_boolean(data_list, (commits, committers, stars, forks, issues, pullrequests))
    target_stats = ["6mo", "total_6mo", "av_6mo", "total_3mo", "av_3mo", "past_month","6mo_corr_coef", "3mo_corr_coef"]
    reference_time_6mo = du.get_time(tz_aware=False, months=6).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_3mo = du.get_time(tz_aware=False, months=3).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_1mo = du.get_time(tz_aware=False, months=1).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_time_current_mo = du.get_time(tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reference_dates = [(reference_time_6mo, 0)]
    for i in range(1, 7):
        reference_dates.append((du.get_time(input_time=reference_time_6mo, add=True, months=i, tz_aware=False).replace(day=1, hour=0, minute=0, second=0, microsecond=0), 0))
    reference_dates = np.asarray(reference_dates)
    stat_list = []
    for datapoint in data_list:
        stat_list += [datapoint + "_" + x for x in target_stats]
    a = dbi.get_time_series_data(timescale=timescale, commits=commits, committers=committers,stars=stars, forks=forks, issues=issues, pullrequests=pullrequests)
    if format_ == 'stats':
        results["org_statistics"] = {v[0]: {'org_stats': {statistic: 0 for statistic in stat_list}} for v in a['unique_repos']}
        for repo_entry in a['unique_repos']:
            results["org_statistics"][repo_entry[0]][repo_entry[1]] = {stat: [] for stat in data_list}
        for stat in data_list:
            for entry in a[stat]:
                results["org_statistics"][entry[2]][entry[3]][stat].append((entry[0], int(entry[1])))
            for account in results["org_statistics"]:
                results["org_statistics"][account]["org_stats"][stat + "_6mo"] = np.copy(reference_dates[:])
                results["org_statistics"][account]["org_stats"][stat + "_3mo"] = np.copy(reference_dates[3:])
        for coin in results["org_statistics"].keys():
            for repo in results["org_statistics"][coin].keys():
                if repo == "org_stats":
                    continue
                for stat in data_list:
                    results["org_statistics"][coin][repo][stat] = np.asarray(results["org_statistics"][coin][repo][stat])
                    if len(results["org_statistics"][coin][repo][stat]) > 0:
                        for entry in results["org_statistics"][coin][repo][stat]:
                            for month in results["org_statistics"][coin]["org_stats"][stat + "_6mo"]:
                                if entry[0] == month[0]:
                                    month[1] += entry[1]
                        results["org_statistics"][coin]['org_stats'][stat + "_3mo"] = np.copy(results["org_statistics"][coin]["org_stats"][stat + "_6mo"][3:])
                        results["org_statistics"][coin][repo][stat + "_total_6mo"] = np.sum(results["org_statistics"][coin][repo][stat][np.where(results["org_statistics"][coin][repo][stat][:, 0] > reference_time_6mo)][:, 1])
                        results["org_statistics"][coin][repo][stat + "_av_6mo"] = results["org_statistics"][coin][repo][stat + "_total_6mo"] / 6
                        results["org_statistics"][coin][repo][stat + "_total_3mo"] = np.sum(results["org_statistics"][coin][repo][stat][np.where(results["org_statistics"][coin][repo][stat][:, 0] > reference_time_3mo)][:, 1])
                        results["org_statistics"][coin][repo][stat + "_av_3mo"] = results["org_statistics"][coin][repo][stat + "_total_3mo"] / 3
                        results["org_statistics"][coin][repo][stat + "_past_month"] = np.sum(results["org_statistics"][coin][repo][stat][np.where(results["org_statistics"][coin][repo][stat][:, 0] > reference_time_1mo) and np.where(results["org_statistics"][coin][repo][stat][:, 0] < reference_time_current_mo)][:,1])
                        results["org_statistics"][coin]['org_stats'][stat + "_total_6mo"] += results["org_statistics"][coin][repo][stat + "_total_6mo"]
                        results["org_statistics"][coin]['org_stats'][stat + "_total_3mo"] += results["org_statistics"][coin][repo][stat + "_total_3mo"]
                    else:
                        results["org_statistics"][coin][repo][stat + "_total_6mo"] = 0
                        results["org_statistics"][coin][repo][stat + "_total_3mo"] = 0
                        results["org_statistics"][coin][repo][stat + "_av_6mo"] = 0
                        results["org_statistics"][coin][repo][stat + "_av_3mo"] = 0
                        results["org_statistics"][coin][repo][stat + "_past_month"] = 0
            for stat in data_list:
                print coin
                print stat
                print results["org_statistics"][coin]["org_stats"][stat + "_6mo"][:, 1]
                print results["org_statistics"][coin]["org_stats"][stat + "_3mo"][:, 1]
                results["org_statistics"][coin]["org_stats"][stat + "_6mo_corr_coef"] = regress_stat(results["org_statistics"][coin]["org_stats"][stat + "_6mo"][:, 1], True)
                results["org_statistics"][coin]["org_stats"][stat + "_3mo_corr_coef"] = regress_stat(results["org_statistics"][coin]["org_stats"][stat + "_3mo"][:, 1], True)
                results["org_statistics"][coin]['org_stats'][stat + "_av_6mo"] = results["org_statistics"][coin]['org_stats'][stat + "_total_6mo"]/6
                results["org_statistics"][coin]['org_stats'][stat + "_av_3mo"] = results["org_statistics"][coin]['org_stats'][stat + "_total_3mo"]/3
    for period in ["_av_6mo", "_av_3mo","_past_month"]:
        stat = "commits"
        x = []
        y = []
        for coin in results["org_statistics"].keys():
            x.append(results["org_statistics"][coin]['org_stats']['commits' + period])
        y = [n for n in x if n > 0]
        if len(y) == 0: continue
        shape, scale = expon.fit(y)
        std_dev = expon.std(scale=scale)
        for coin in results["org_statistics"].keys():
            results["org_statistics"][coin]["org_stats"][stat + period + "_percentile"] = expon.cdf(results["org_statistics"][coin]['org_stats'][stat + period], scale=scale)
            results["org_statistics"][coin]["org_stats"][stat + period + "_std_devs"] = results["org_statistics"][coin]['org_stats'][stat + period]/std_dev - 1
        results["aggregate_statistics"]["number_of_github_orgs"] = len(results["org_statistics"])
        results["aggregate_statistics"][stat + period] = {"shape":shape, "fit_type":"Exponential", "scale":scale, "std_dev":std_dev}
        results["aggregate_statistics"][stat + period] = {"shape": shape, "fit_type": "Exponential", "scale": scale, "std_dev": std_dev}
    return results

def prune_data(source_data, stat=None, period=None, exclude_zeros=False, custom_input = None):
    data = []
    if custom_input != None: statistic = custom_input
    else: statistic = stat + "_" + period
    for coin in source_data['org_statistics'].keys():
        data.append(source_data['org_statistics'][coin]['org_stats'][statistic])
    if exclude_zeros == True:
        data = [n for n in data if n > 0]
    return data

def create_histogram(data, bins=10):
    plt.hist(data, bins=bins)
    plt.show()