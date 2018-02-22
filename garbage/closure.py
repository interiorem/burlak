import time


def main_loop():

    var_one, var_two = 1, 2
    var_bool = False

    for i in xrange(10):
        var_one += 1
        var_two += 2

        var_bool ^= 1

        def summup():
            return var_one + var_two, var_bool

        print '{} + {} = {}'.format(var_one, var_two, summup())

        time.sleep(1)


main_loop()
