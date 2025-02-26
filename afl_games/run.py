from pathlib import Path
import yaml
from importlib import import_module

yaml_file_path = __file__.replace(".py", ".yaml")
if Path(yaml_file_path).exists():
    with open(yaml_file_path) as yaml_file:
        pipeline_config = yaml.safe_load(yaml_file)
        pipeline_list = [pipeline for pipeline in pipeline_config.get("pipelines")]
        # config = pipeline_config.get("config")
else:
    raise Exception(
        f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
    )
print(pipeline_list)
for pipeline in pipeline_list:
    pipeline_name = pipeline.get("name")
    pipeline_config = pipeline.get("config")
    module = import_module(name=f".{pipeline_name}", package="main.pipelines")
    module.run_pipeline(pipeline_config)