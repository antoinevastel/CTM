from scheduler import ScenarioManager

def main():
    scenario_manager = ScenarioManager("my_tasks_package", "./tasks_definitions/scenario.json")
    scenario_manager.read_scenario()
    scenario_manager.run_scenario()

if __name__ == "__main__":
    main()

