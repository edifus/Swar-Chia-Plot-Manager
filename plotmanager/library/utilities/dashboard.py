import os
import json
import requests
import threading
import time
import logging

from datetime import datetime, timedelta
from requests.exceptions import HTTPError
from plotmanager.library.parse.configuration import get_config_info
from plotmanager.library.utilities.jobs import load_jobs
from plotmanager.library.utilities.log import analyze_log_dates, check_log_progress
from plotmanager.library.utilities.processes import get_running_plots
from plotmanager.library.utilities.print import _get_row_info

chia_location, log_directory, config_jobs, manager_check_interval, max_concurrent, max_for_phase_1, \
    minimum_minutes_between_jobs, progress_settings, notification_settings, debug_level, view_settings, \
    instrumentation_settings, dashboard_settings = get_config_info()

# dashboard logging
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
dashboard_logging = extra_logger('dashboard_logs', 'dashboard.log')

def extra_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger
# end dashboard logging

def dashboard_thread():
    newThread = threading.Thread(target=dashboard_update_loop, args=())
    newThread.start()

def dashboard_update_loop():
    try:
        while True:
            dashboard_logging.info('Updating dashboard ...', flush=True)
            update_dashboard()
            dashboard_logging.info('Sleeping 60 seconds ...', flush=True)
            time.sleep(60)
            dashboard_logging.info('Waking up ...', flush=True)
    except:
        sys.exit()

def update_dashboard():
    analysis = {'files': {}}
    running_work = {}
    analysis = analyze_log_dates(log_directory=log_directory, analysis=analysis)
    jobs = load_jobs(config_jobs)
    jobs, running_work = get_running_plots(jobs=jobs, running_work=running_work,
                        instrumentation_settings=instrumentation_settings)
    check_log_progress(jobs=jobs, running_work=running_work, progress_settings=progress_settings,
                        notification_settings=notification_settings, view_settings=view_settings,
                        instrumentation_settings=instrumentation_settings)
    get_job_data(jobs=jobs, running_work=running_work, analysis=analysis)


def _get_row_info(pid, running_work):
    work = running_work[pid]
    phase_times = work.phase_times
    elapsed_time = (datetime.now() - work.datetime_start)
    phase_time_log = []
    for i in range(1, 5):
        if phase_times.get(i):
            phase_time_log.append(phase_times.get(i))

    row = [
        work.job.name if work.job else '?',
        work.k_size,
        pid,
        work.datetime_start,
        elapsed_time,
        work.current_phase,
        ' / '.join(phase_time_log),
        work.progress
    ]
    return [str(cell) for cell in row]

def get_job_data(jobs, running_work, analysis):
    rows = []
    added_pids = []
    for job in jobs:
        for pid in job.running_work:
            if pid not in running_work:
                continue
            rows.append(_get_row_info(pid, running_work))
            added_pids.append(pid)
    for pid in running_work.keys():
        if pid in added_pids:
            continue
        rows.append(_get_row_info(pid, running_work))
        added_pids.append(pid)
    rows.sort(key=lambda x: (x[4]), reverse=True)
    for i in range(len(rows)):
        rows[i] = [str(i+1)] + rows[i]
    dashboard_logging.info("Connecting to dashboard ...", flush=True)
    dashboard_request(plots = rows, analysis=analysis)


def set_dashboard_data(plots):
    data = []
    for plot in plots:
        arr = {
            "id": plot[3],
            "startedAt": plot[4],
            "state": "RUNNING",
            "kSize": plot[2],
            "phase": plot[6],
            "progress": float(plot[8].strip('%'))/100
        }
        data.append(arr)
    return data


def dashboard_request(plots, analysis):

    data = {
        "plotter": {
            "completedPlotsToday": analysis["summary"].get(datetime.now().date(), 0),
            "completedPlotsYesterday": analysis["summary"].get(datetime.now().date() - timedelta(days=1), 0),
            "jobs": set_dashboard_data(plots)
        }
    }
    data = json.dumps(data)
    url = dashboard_settings.get('dashboard_update_url')
    headers = {
        'Authorization': "Bearer " + dashboard_settings.get('dashboard_api_key'),
        'Content-Type': 'application/json'
    }
    logging.basicConfig(filename='dashboard.log', format='%(asctime)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=debug_level, force=True)
    try:
        response = requests.patch(url, headers=headers, data=data)
        if response.status_code == 204:
            dashboard_status = "Connected"
        elif  response.status_code == 429:
            dashboard_status = "Too many Requests. Slow down."
            dashboard_logging.info(dashboard_status + str(response))
        else:
            response.raise_for_status()
    except HTTPError:
        if response.status_code == 401:
            dashboard_status = "Unauthorized. Possibly invalid API key?"
            dashboard_logging.info(dashboard_status + str(response))
        else:
            dashboard_status = "Unable to connect."
            dashboard_logging.info(dashboard_status + str(response))
    except requests.exceptions.ConnectionError:
        dashboard_status = "Connection Error. Chia Dashboard may not be responding."
        dashboard_logging.info(dashboard_status)
    return dashboard_status
