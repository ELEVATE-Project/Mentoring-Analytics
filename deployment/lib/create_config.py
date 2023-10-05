import json , sys

# Specify the file path
file_path =  sys.argv[1]
configFile = "config.ini"
# Open the file for reading
with open(file_path, "r") as json_file:
    # Load the JSON data from the file
    data = json.load(json_file)

# rewrite the config file
with open(configFile, "w") as conf:
    conf.write("\n")

for key,value in data.items():

    # Open the file for reading
    with open(configFile, "a") as conf:
        conf.write("["+str(key)+"]")
        conf.write("\n")
    for innerKey,innerValue in value.items():
        # Open the file for reading
        with open(configFile, "a") as innerConf:
            # tempWriter = innerKey , " = " , innerValue
            innerConf.write(f"{innerKey} = {innerValue}")
            
            innerConf.write("\n")
