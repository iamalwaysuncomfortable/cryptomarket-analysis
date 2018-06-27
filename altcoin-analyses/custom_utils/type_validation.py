import log_service.logger_factory as lf
logging = lf.get_loggly_logger(__name__)

def validate_type(test_var, ref_types):
    '''
    Function uses isinstance method to compare the type of the variable, throws error if type does not match
    
    :param test_var: The object/variable to test type
    :param ref_types: Reference type(s) variable should resolve to
    :return: 
    '''
    if not isinstance(test_var, ref_types):
        msg = "Error arg %s was of type %s required types are %s" % (str(test_var), type(test_var), ref_types)
        logging.warn(msg)
        raise TypeError(msg)
    else: return True

def validate_list_element_types(input_table, ref_types):
    if validate_type(input_table, (list, tuple)) and all(isinstance(v, ref_types) for v in input_table):
        return True
    else: return False