

def flatten_dict(d, result):

    if not isinstance(d, dict):
        return

    node_stack = [(k, v) for k, v in d.iteritems()]

    while node_stack:
        k, item = node_stack.pop()
        path_stack.append(k)

        if item and isinstance(item, dict):
            node_stack.extend([(k, v) for k, v in item.iteritems()])
        else:
            result.append(('.'.join(path_stack), v))
            path_stack.pop()



def flatten_dict_rec(d, current_path, result):
    if not d:
        return

    for k, v in d.iteritems():
        if v and isinstance(v, dict):
            new_path = []
            new_path.extend(current_path)
            new_path.append(k)

            flatten_dict_rec(v, new_path, result)
        else:
            result.append(('.'.join(current_path + [k]), v))


d = dict(a=1,b=2,c=dict(z=100,x=500,y=60,d=dict(q=42)), d=dict(r=dict()))

result = []

flatten_dict_rec(d, [], result)


print 'result for d = {}'.format(d)
print '   rec: \t{}'.format(result)


flatten_dict(d, result)
print 'nonrec: \t{}'.format(result)
