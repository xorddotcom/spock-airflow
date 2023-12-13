from airflow.providers.slack.operators.slack import SlackAPIPostOperator

SLACK_CHANNEL='#spock-jenkins-notifications'

def notify_success(context):
        
    attachments = [{
        'text': (
            f'''
            Task Successful >> Protocol: {context.get('task_instance').dag_id} >> CC: <@U02DZLLR01L>, <@U01QFUN1F2N>
            '''
            ),
        'color': '#90EE90'
    }]
    _notify = SlackAPIPostOperator(
        task_id='success_notification',
        slack_conn_id='slack',
        channel=SLACK_CHANNEL,
        icon_url='https://res.cloudinary.com/dalayuyv1/image/upload/v1702479633/download_l0fg4f.png',
        attachments=attachments,
        text=''
    )
    return _notify.execute(context=context)

def notify_failure(context): 
    
    attachments = [{
        'text': (
            f'''
            Task Failed >> Protocol: {context.get('task_instance').dag_id} >> CC: <@U02DZLLR01L>, <@U01QFUN1F2N>
            '''
            ),
        'color': '#FF474C'
    }]
    _notify = SlackAPIPostOperator(
        task_id='failure_notification',
        slack_conn_id='slack',
        channel=SLACK_CHANNEL,
        icon_url='https://res.cloudinary.com/dalayuyv1/image/upload/v1702479633/download_l0fg4f.png',
        attachments=attachments,
        text=''
    )
    return _notify.execute(context=context)