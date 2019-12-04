from itertools import permutations


def get_running_tasks(task):
    """
    Given a task, get all tasks that must be running or finished executing for this task to be running

    :param task: A task_id
    :return: A list of all tasks that are running or finished executing for this task to be running
    """
    all_running_tasks = [task]
    prereq_tasks = task_to_prereqs[task]

    if not prereq_tasks:
        return all_running_tasks

    for prereqs in [get_running_tasks(x) for x in prereq_tasks]:
        all_running_tasks.extend(prereqs)

    return all_running_tasks


def build_pipeline(starting_tasks, pipeline, running_tasks):
    """
    Find a path from the end of the existing pipeline to one of the starting tasks

    :param starting_tasks: A list of starting tasks
    :param pipeline: An list of tasks from the goal task followed by some amount of prerequisite tasks
    :param running_tasks: A list of tasks that are already running or executed
    :return: A list with all prerequisite tasks needed to run the goal task in reverse order
    """
    # If one of our starting tasks is in our pipeline, our pipeline is complete
    if set(starting_tasks).intersection(pipeline):
        return pipeline

    # Get the last task in our pipeline and find all of its prerequisites
    last_task = pipeline[-1]
    prereq_tasks = task_to_prereqs[last_task]

    # If there are no prerequisite tasks, we cannot extend this pipeline any further
    if not prereq_tasks:
        return pipeline

    running_tasks.extend(prereq_tasks)

    for prereq in list(permutations(prereq_tasks)):
        pipeline.extend(prereq)
        return build_pipeline(starting_tasks, pipeline, running_tasks)


def get_pipeline(starting_tasks, goal_task):
    """
    Get a pipeline of tasks that must be run in order to run the goal_task.
    Start pipeline by running all the given starting_tasks.

    :param starting_tasks: A list of tasks that will be started running first
    :param goal_task: The task that will pipeline is attempting to run
    :return: A list representing the order to run tasks in such that every task will have the prerequisites it needs
        to run, the final task in this list will be the goal_task
    """
    # Get all the tasks that must be running given that the starting_tasks are running
    running_tasks = []
    for starting_task in starting_tasks:
        running = get_running_tasks(starting_task)
        running_tasks.extend(running)

    # Initialize pipeline with the goal_task
    pipeline = [goal_task]

    # Build the pipeline backwards from the goal_task until we find a way to get it running
    # with all of its prerequisites
    reverse_pipeline = build_pipeline(starting_tasks, pipeline, running_tasks)

    return reverse_pipeline[::-1]


if __name__ == "__main__":
    # Get all task_ids from file
    with open('task_ids.txt') as task_ids_file:
        task_ids = task_ids_file.readline()
        task_list = task_ids.split(',')

    # Create a dictionary to store each task and a list of its prerequisite tasks
    task_to_prereqs = {int(task): [] for task in task_list}

    # Populate task_to_prereqs with data from relations file
    with open('relations.txt') as relations_file:
        for relation in relations_file:
            prereq_task, task = [int(x) for x in relation.split('->')]

            if task in task_to_prereqs:
                task_to_prereqs[task].append(prereq_task)
            else:
                raise Exception('Relation found containing task that is not in task_list.txt')

    # Get input parameters for this problem
    starting_tasks = [73]
    goal_task = 36

    # Validation
    for task in starting_tasks:
        if task not in task_to_prereqs:
            raise Exception('Starting task {task} does not exist in task_id.txt'.format(task=task))

    if goal_task not in task_to_prereqs:
        raise Exception('Goal task {task} does not exist in task_id.txt'.format(task=goal_task))

    print(get_pipeline(starting_tasks, goal_task))
