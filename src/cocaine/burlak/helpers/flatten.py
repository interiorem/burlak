

#
# TODO: more tests
#
def flatten_dict(d, separtor='.'):
    '''flatten_dict

    Return plain list of tuples with concatinated paths to leafs.

    '''
    result = []

    if not (d and isinstance(d, dict)):
        return result

    node_stack = [([k], v) for k, v in d.iteritems()]

    while node_stack:
        path, item = node_stack.pop()

        if item and isinstance(item, dict):
            node_stack.extend([(path + [k], v) for k, v in item.iteritems()])
        else:
            result.append((separtor.join(path), item))

    return result


#
# Note: not tested at all!
#
def flatten_dict_rec(d, separtor='.'):

    def flatten_dict_rec_impl(d, current_path, result):
        if not (d and isinstance(d, dict)):
            return

        for k, v in d.iteritems():
            if v and isinstance(v, dict):
                new_path = []
                new_path.extend(current_path)
                new_path.append(k)

                flatten_dict_rec_impl(v, new_path, result)
            else:
                result.append((separtor.join(current_path + [k]), v))

    current_path = []
    result = []

    flatten_dict_rec_impl(d, current_path, result)

    return result
