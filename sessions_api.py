##
## Vivek Bhardwaj (Hubspot Opening Assessment)
## December 19th, 2021
##

import requests, json

GET_URL = "https://candidate.hubteam.com/candidateTest/v3/problem/dataset?userKey=6ff400eda6456f7101c0e814bd28"
POST_URL = "https://candidate.hubteam.com/candidateTest/v3/problem/result?userKey=6ff400eda6456f7101c0e814bd28"


"""
main() purpose:
1) gather json event data
2) parse raw data to get sessionsByUser
3) make post request
"""
def main():
    events = requests.get(GET_URL).json()["events"]
    result = post_request(fetch_raw_data(events))
    print(result.text, f" | Status Code: {result.status_code}")


"""
fetch_raw_data(events) purpose:
1) compile a set of all sessions based on user id (uid)
"""
def fetch_raw_data(events):
    sessions_by_user = {}
    
    for event in events[:]:
        uid = event["visitorId"]
        if uid not in sessions_by_user: 
            sessions_by_user[uid] = []
        sessions = sessions_by_user[uid]
        sessions.append(event)
    
    for uid in sessions_by_user:
        sessions_by_user[uid] = fetch_session_data(sessions_by_user[uid])
        
    return {"sessionsByUser": sessions_by_user}


"""
fetch_session_data(sessions) purpose: 
1) use the compiled list set of sessions from fetch_raw_data to create the expected post result
2) if list of sessions is empty, append a new list with the first session data
3) elif check to see if the timestamps are within the 10 minute period, and accordingly append
4) otherwise, append the duration, pages, and startTime for the respect session
"""
def fetch_session_data(sessions):
    sessions.sort(key=lambda session: session["timestamp"])
    current_time = 0
    list_of_sessions = []
    for session in sessions:
        if len(list_of_sessions) <= 0:
            list_of_sessions.append(
                {
                    "duration": 0,
                    "pages": [session["url"]],
                    "startTime": session["timestamp"],
                }
            )
        elif abs(session["timestamp"] - current_time) <= 600000:
            list_of_sessions[-1]["duration"] = (
                session["timestamp"] - list_of_sessions[-1]["startTime"]
            )
            list_of_sessions[-1]["pages"].append(session["url"])
        else:
            list_of_sessions.append(
                {
                    "duration": 0,
                    "pages": [session["url"]],
                    "startTime": session["timestamp"],
                }
            )
        current_time = session["timestamp"]
    return list_of_sessions


"""
post_request(analytics) purpose:
1) make the final post request
"""
def post_request(analytics):
    return requests.post(POST_URL, data=json.dumps(analytics))


main()