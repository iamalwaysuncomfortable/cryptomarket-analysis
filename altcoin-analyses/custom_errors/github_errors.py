###Custom Errors
class GithubAPIError(Exception):
    'If a misc Github API error is reached pass error'
    def __init__(self, data, msg=None):
        if msg is None:
            # Default Message
            msg = "Github API Error!"
        super(GithubAPIError, self).__init__(msg)
        self.data = data

class GithubPaginationError(GithubAPIError):
    'If pagination of Github records backwards in time fails throw error'
    def __init__(self, result, query, org_data, cursors, depth, msg=None):
        if msg is None:
            msg = "Back pagination error at depth " + str(depth)
        data = [result, query, org_data, cursors, depth, msg]
        super(GithubAPIError, self).__init__(data, msg)
        self.query = query
        self.result = result
        self.org_data = org_data
        self.cursors = cursors
        self.depth = depth
        self.data = data

class GithubAPILimitExceededError(GithubAPIError):
    'If Github API Limit Exceed pass error'
    def __init__(self, resetAt, remaining, cost, msg=None):
        data = [resetAt, remaining, cost]
        if msg is None:
            msg = "cost of query was %s, remaining hourly rate limit is %s, limit exceeded, reset at %s" % (cost, remaining, resetAt)
        super(GithubAPILimitExceededError, self).__init__(data, msg)
        self.resetAt = resetAt
        self.remaining = remaining
        self.cost = cost

class GithubAPIBadQueryError(GithubAPIError):
    'Throw error if query returns malformed result'
    def __init__(self, result, query, msg=None):
        data = [result, query]
        if msg is None:
            msg = "Query returned bad result"
        super(GithubAPIError, self).__init__(data, msg)
        self.query = query
        self.result = result