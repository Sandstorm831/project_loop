from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, Response
from datetime import datetime
import uuid
from src.algo import report_processor, result_loc
from src.volatile import processing_status, requestedIdMap
import threading

previous_processing_datetime = None
app = FastAPI()


def datetimeDiff(after: datetime, before: datetime):
    time_diff = after - before
    hours = time_diff.total_seconds() // 3600
    return hours


@app.get("/trigger_report")
async def trigger_report():
    current = datetime.now()
    uid = str(uuid.uuid4())
    global previous_processing_datetime
    global processing_status
    global requestedIdMap
    requestedIdMap[uid] = current
    if previous_processing_datetime is None:
        previous_processing_datetime = current
        threading.Thread(target=report_processor).start()
        # print(processing_status)
        processing_status["status"] = 1
    elif datetimeDiff(current, previous_processing_datetime) >= 1.0:
        previous_processing_datetime = current
        threading.Thread(target=report_processor).start()
        processing_status["status"] = 1
    return {"status_id": uid}


@app.get("/get_report")
async def get_report(report_id: str = ""):
    global previous_processing_datetime
    global processing_status
    global requestedIdMap
    if report_id == "":
        return Response(status_code=422, content="Empty/Invalid report_id")
    elif report_id not in requestedIdMap:
        # print(f'report_id: {report_id}')
        # print(requestedIdMap)
        return Response(status_code=422, content="Empty/Invalid report_id")
    elif processing_status["status"] == 1:
        return {"status": "Running"}
    elif processing_status == 3:
        del requestedIdMap[report_id]
        return {"status": "Error occured, please try again later"}
    else:
        del requestedIdMap[report_id]
        return FileResponse(path=result_loc, media_type='text/csv')
