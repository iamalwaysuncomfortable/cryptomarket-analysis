import res.env_config as env
env.set_master_config()
import data_scraping.gitservice.api_communication as gitapi
import log_service.logger_factory as lf
from custom_utils.datautils import boldprint

logging = lf.get_loggly_logger(__name__)
lf.launch_logging_service()

def test_http_repo_query_correctness_for_orgs():
    boldprint("Test org repos query runs correctly")
    data, status = gitapi.repo_query_http(account_type="org", account_name ="ethereum", page =1, retries=3)
    assert status == 200
    assert len(data) == 30

def test_http_repo_query_pagination_for_orgs():
    boldprint("Test org repos query with multiple pages runs correctly")
    data, status = gitapi.repo_query_http(account_type="org", account_name ="ethereum", page =2, retries=3)
    assert status == 200
    assert len(data) == 30

def test_http_repo_query_pagination_end_for_orgs():
    boldprint("Test that for more pages than exist returns 0 length result")
    data, status = gitapi.repo_query_http(account_type="org", account_name ="ethereum", page = 20, retries=3)
    assert status == 200
    assert len(data) == 0

def test_http_repo_query_correctness_for_users():
    boldprint("Test user repos query runs correctly")
    data, status = gitapi.repo_query_http(account_type="user", account_name="GlitchyBritches", page=1, retries=3)
    assert status == 200
    assert len(data) == 9