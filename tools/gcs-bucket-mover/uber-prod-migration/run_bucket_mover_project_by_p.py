import subprocess
import os
from multiprocessing import Pool, cpu_count


def run_command(command):
    buckets, project = command[3:5]
    log_dir = "log"
    log_file = f"{log_dir}/{project}_{buckets}.log"
    os.makedirs(log_dir, exist_ok=True)
    print(f"Running command: {' '.join(command)}")
    with open(log_file, "w") as f:
        process = subprocess.Popen(command, stdout=f, stderr=subprocess.STDOUT)
        return_code = process.wait()
    if return_code != 0:
        print(f"Failed !!!!! project: {project}, buckjet: {buckets} ")
    return return_code


if __name__ == '__main__':
    with open("staging.input", "r") as f:
        buckets_by_project = {}
        for line in f:
            bucketsect_name, project = line.strip().split()
            if project not in buckets_by_project:
                buckets_by_project[project] = []
            buckets_by_project[project].append(bucketsect_name)

        # Sort projects in alphabetical order
        sorted_projects = sorted(buckets_by_project.keys())

        # Run commands for each project one by one
        success = True
        with Pool(processes=80) as pool:
            commands = []
            for project in sorted_projects:
                print(f"Running commands for project {project}")
                for bucket in buckets_by_project[project]:
                    command = ["/home/user/professional-services/tools/gcs-bucket-mover/bin/bucket_mover",
                               "--config", "/home/user/professional-services/tools/gcs-bucket-mover/config.yaml",
                               bucket, project, project]
                    commands.append(command)
                results = pool.map(run_command, commands)
                if any(result != 0 for result in results):
                    success = False
                    break

        if success:
            print("All commands have completed successfully")
        else:
            print("There were errors running the commands")
