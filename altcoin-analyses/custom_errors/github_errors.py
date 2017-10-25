###Custom Errors
class GithubAPIError(Exception):
    'If a misc Github API error is reached pass error'
    def __init__(self, data, status, msg=None):
        if msg is None:
            # Default Message
            msg = "Github API Error! Status code was: %s" %(status)
        super(GithubAPIError, self).__init__(msg)
        self.data = data
        self.status = status

class GithubPaginationError(GithubAPIError):
    'If pagination of Github records backwards in time fails throw error'
    def __init__(self, source_error, source_message, result, query, cursors, depth, msg=None):
        if msg is None:
            msg = "Back pagination error at depth " + str(depth) + " base error was: "
        data = [source_error, source_message, result, query, cursors, depth, msg + source_message]
        super(GithubAPIError, self).__init__(data, msg)
        self.source_error = source_error
        self.message = msg + source_message
        self.query = query
        self.result = result
        self.cursors = cursors
        self.depth = depth
        self.data = data

class LimitExceededDuringPaginationError(GithubPaginationError):
    'If backwards pagination in time fails due to the API limit exceeded, throw error'
    def __init__(self, resetAt, result, query, org_data, cursors, depth, msg=None):
        if msg is None:
            msg = "Github Limit exceeded at depth " + str(depth)
        data = [result, query, org_data, cursors, depth, msg]
        super(GithubPaginationError, self).__init__(data, msg)
        self.resetAt = resetAt
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
    def __init__(self, result, query, status, msg=None):
        data = [result, query]
        if msg is None:
            msg = "Query returned bad result with http status code: %s, and query %s" %(status, query)
        super(GithubAPIError, self).__init__(data, msg)
        self.query = query
        self.status = status
        self.result = result