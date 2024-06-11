import argparse
import glob
import json
import os
from contextlib import closing
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Process dbt manifest and generate Airflow DAG.")
    parser.add_argument(
        "--manifest_path",
        default="plugins/dbt_pg_project/target/manifest.json",
        required=False,
        help="Path to the dbt manifest file",
    )
    parser.add_argument(
        "--project_path", default="plugins/dbt_pg_project", required=False, help="Path to the dbt project"
    )
    parser.add_argument(
        "--profile_path", default="plugins/dbt_pg_project", required=False, help="Path to the dbt profile"
    )
    parser.add_argument(
        "--dag_folder_path",
        default="dags/dbt/",
        required=False,
        help="Folder path where generated DAGs will be saved",
    )

    parser.add_argument(
        "--keep_previous_dags",
        action="store_true",
        help="Flag to not erase the dags folder before the generation of new DAGs",
    )

    return parser.parse_args()


def get_manifest(manifest_path):
    with open(manifest_path) as f:
        manifest = json.load(f)
    return manifest


def generate_airflow_task(node_type, node_name, dbt_project_path, dbt_profile_path, indentation="    "):
    type2command = {"model": "run", "test": "test"}
    dbt_command = type2command[node_type]
    task_id = f"{dbt_command}_{node_name}"
    task = f"""
{indentation}{node_name}_task = BashOperator(
{indentation}{indentation}task_id='{task_id}',
{indentation}{indentation}bash_command='cd {dbt_project_path} && dbt {dbt_command} \\
{indentation}{indentation}--profiles-dir {dbt_profile_path} --select {node_name}',
{indentation}{indentation}env={{'DBT_POSTGRES_PASSWORD': Variable.get("dex_demo_pg_password")}}
{indentation})"""
    return task


def generate_airflow_schedule(model_dependencies, nodes_type_map):
    if not model_dependencies:
        return "'@daily'"

    model_dependencies = [
        f"{model}_model" if model in nodes_type_map else f"{model}" for model in model_dependencies
    ]

    schedule_dataset = ", ".join([f"Dataset('{dep}')" for dep in model_dependencies])
    return f"[{schedule_dataset}]"


def generate_airflow_dag_id(node, nodes_type_map):
    return f"run_{nodes_type_map[node]}__{node}"


def create_dependencies(node, model_tests_dependencies: list, indentation="    "):
    if not model_tests_dependencies:
        return [f"{indentation}{node}_task >> end_task"]

    dependencies = []
    for test_dependecy in model_tests_dependencies:
        dependencies.append(f"{indentation}{node}_task >> {test_dependecy}_task >> end_task")
    return dependencies


def parse_manifest(manifest):
    nodes = manifest["nodes"]
    nodes_type_map = {node["name"]: node["resource_type"] for _, node in nodes.items()}

    datasets_map = {
        node["name"]: node["meta"]["datasets_trigger"].split(",")
        for _, node in nodes.items()
        if "datasets_trigger" in node["meta"]
    }

    upstreams = {}
    for _, node in nodes.items():
        node_name = node["name"]
        depends_on = node["depends_on"]
        node_dependencies = depends_on.get("nodes", [])
        node_dependencies = [nodes[dep]["name"] for dep in node_dependencies if dep.startswith("model")]
        upstreams[node_name] = node_dependencies
    return upstreams, nodes_type_map, datasets_map


def get_models_dependecies(upstreams, nodes_type_map, datasets_map):
    tests_dependecies = {}
    for node, upstream in upstreams.items():
        if nodes_type_map[node] != "test":
            continue
        for dep in upstream:
            _map = tests_dependecies.get(dep, [])
            _map.append(node)
            tests_dependecies[dep] = _map

    models_dependecies = {
        node: [*deps, *datasets_map.get(node, [])]
        for node, deps in upstreams.items()
        if nodes_type_map[node] == "model"
    }

    dependencies = {
        node: {
            "tests_dependecies": tests_dependecies.get(node, []),
            "model_dependecies": models_dependecies[node],
        }
        for node in models_dependecies
    }

    return dependencies


def generate_airflow_dags(
    dag_folder_path, manifest, dbt_project_path, dbt_profile_path, clean_folder_before_generate: bool
):
    upstreams, nodes_type_map, datasets_map = parse_manifest(manifest)
    models_dependecies = get_models_dependecies(upstreams, nodes_type_map, datasets_map)

    dag_path = Path(dag_folder_path)
    if not dag_path.exists():
        dag_path.mkdir(parents=True, exist_ok=True)

    files_to_write = {}

    for node, dependencies in models_dependecies.items():
        tasks_str = "\n".join(
            [
                generate_airflow_task(nodes_type_map[dbt_node], dbt_node, dbt_project_path, dbt_profile_path)
                for dbt_node in [node, *dependencies["tests_dependecies"]]
            ]
        )
        tests_dependencies = create_dependencies(node, dependencies["tests_dependecies"], indentation="    ")
        dependencies_str = "\n".join(tests_dependencies) if tests_dependencies else ""

        dag_name = generate_airflow_dag_id(node, nodes_type_map)

        with closing(open(Path(__file__).parent.joinpath("./dag_template.txt"))) as dag_template_file:
            dag_content = dag_template_file.read()
        assert dag_content is not None

        files_to_write[dag_path.joinpath(f"{dag_name}.py")] = dag_content.format(
            dag_id=dag_name,
            tasks_str=tasks_str,
            node_type=nodes_type_map[node],
            dependencies_str=dependencies_str,
            dag_trigger=generate_airflow_schedule(dependencies["model_dependecies"], nodes_type_map),
            outlet_dataset=f"{node}_model",
        )

    if clean_folder_before_generate:
        files = glob.glob(f"{Path(dag_folder_path).absolute().as_posix()}/*.py")
        for f in files:
            os.remove(f)

    for filename, file_content in files_to_write.items():
        with closing(open(filename, "w")) as file:
            file.write(file_content)


if __name__ == "__main__":
    args = parse_args()
    manifest = get_manifest(args.manifest_path)
    generate_airflow_dags(
        args.dag_folder_path,
        manifest,
        args.project_path,
        args.profile_path,
        not args.keep_previous_dags,
    )
