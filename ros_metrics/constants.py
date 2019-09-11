import yaml

constants = yaml.load(open('data/constants.yaml'))

ros1_distros = constants['ros1']
ros2_distros = constants['ros2']
distros = constants['ros1'] + constants['ros2']
os_list = constants['ubuntu'] + constants['debian']
architectures = constants['architectures']

countries = yaml.load(open('data/countries.yaml'))
