import os
import shutil
import argparse

# DESIGNED FOR COMUNICA v4-3-0

class ExperimentOptions:
    def __init__(
        self,
        rate_limit: bool = False,
        ask: bool = False,
        count: bool = False,
        void: bool = False,
        get: bool = False,
        large_void: bool = False,
        block_size: str = "",
        bindings: str = ""
    ):
        self.rate_limit: bool = rate_limit
        self.ask: bool = ask
        self.count: bool = count
        self.void: bool = void
        self.large_void: bool = large_void
        self.block_size: str = block_size
        self.bindings: str = bindings

# Experiments and their specific config options
experiment_options_dict = {
    "EX1": ExperimentOptions(
        rate_limit=True,
        ask=True,
        count=False,
        void=False,
        get=False,
        large_void=False,
        block_size="default",
        bindings="default"
    ),
    "EX1g": ExperimentOptions(
        rate_limit=True,
        ask=True,
        count=False,
        void=False,
        get=True,
        large_void=False,
        block_size="default",
        bindings="default"
    ),
    "EX2": ExperimentOptions(
        rate_limit=True,
        ask=True,
        count=True,
        void=False,
        get=False,
        large_void=False,
        block_size="default",
        bindings="default"
    ),
    "EX3": ExperimentOptions(
        rate_limit=True,
        ask=True,
        count=False,
        void=False,
        get=False,
        large_void=False,
        block_size="default",
        bindings="default"
    ),
    "EX4": ExperimentOptions(
        rate_limit=True,
        ask=True,
        count=False,
        void=False,
        get=False,
        large_void=False,
        block_size="default",
        bindings="default"
    ),
}

def changeRateLimit(v1):
    if v1:
        src = os.path.join(os.getcwd(), 'config/rate-limit-on/actors-limit-rate.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/http/actors-limit-rate.json')
        shutil.copyfile(src, dst)
        print(f"\tRate limiting: ON ✅")
    else:
        src = os.path.join(os.getcwd(), 'config/rate-limit-off/actors-limit-rate.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/http/actors-limit-rate.json')
        shutil.copyfile(src, dst)
        print(f"\tRate limiting: OFF ❌")

def changeAsk(v2):
    if v2:
        src = os.path.join(os.getcwd(), 'config/ask/actors-v4-3-0.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/optimize-query-operation/actors-v4-3-0.json')
        shutil.copyfile(src, dst)
        print(f"\tASK: ON ✅")
    else:
        print(f"\tASK: DEFAULT (OFF ❌)")

def countAndVoid(c, lv, g):
    # ONLY COUNT
    if c and not lv and not g:
        src = os.path.join(os.getcwd(), 'config/only-count/actors.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/query-source-identify-hypermedia/actors.json')
        shutil.copyfile(src, dst)
        print(f"\tOnly GET: OFF ❌\tVoID (large): OFF ❌\n\tCOUNT: ON ✅")
    # ONLY COUNT -- GET
    if c and not lv and g:
        src = os.path.join(os.getcwd(), 'config/only-count-get/actors.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/query-source-identify-hypermedia/actors.json')
        shutil.copyfile(src, dst)
        print(f"\tOnly GET: ON ✅\n\tVoID (large): OFF ❌\n\tCOUNT: ON ✅")
    # ONLY LARGE VoID
    elif not c and lv:
        src = os.path.join(os.getcwd(), 'config/void-large/actors.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/query-source-identify-hypermedia/actors.json')
        shutil.copyfile(src, dst)
        print(f"\tVoID (large): ON ✅\n\tCOUNT: OFF ❌")
    # NO COUNT OR LARGE VoID
    elif not c and not lv:
        src = os.path.join(os.getcwd(), 'config/no-count/actors.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/query-source-identify-hypermedia/actors.json')
        shutil.copyfile(src, dst)
        print(f"\tVoID (large): OFF ❌\n\tCOUNT: OFF ❌")
    else:
        print(f"\tVoID (large): DEFAULT (OFF) ❌\n\tCOUNT: DEFAULT (ON) ✅")

def generalVoid(v3):
    if v3:
        src = os.path.join(os.getcwd(), 'config/void/actors-v4-1-0.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/rdf-metadata-extract/actors-v4-1-0.json')
        shutil.copyfile(src, dst)
        print(f"\tVOID (general): ON")
    else:
        src = os.path.join(os.getcwd(), 'config/no-void/actors-v4-1-0.json')
        dst = os.path.join(os.getcwd(), 'comunica/engines/config-query-sparql/config/rdf-metadata-extract/actors-v4-1-0.json')
        shutil.copyfile(src, dst)
        print(f"\tVOID (general): OFF ❌")

def changeComunicaConfigs(config_options: ExperimentOptions):
    """Top level config changes method"""
    changeRateLimit(config_options.rate_limit)
    changeAsk(config_options.ask)
    generalVoid(config_options.void)
    countAndVoid(config_options.count, config_options.large_void)
    print(f"Configuration changes have been applied 🎉")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Automatically alter Comunica configs for various experiments.")
    parser.add_argument("-e", "--experiment", type=str, required=True, help="The experiment configs you want.")
    args = parser.parse_args()
    experiment = args.experiment.upper()

    try:
        print(f"Changing Comunica configs for experiment: {experiment}")
        changeComunicaConfigs(experiment_options_dict[experiment])

    except KeyError:
        print(f"Unknown experiment: {experiment}. Please use EX1, EX2, EX3, or EX4.")
