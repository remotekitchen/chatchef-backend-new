def action_saver(action, text):
    action.logs += f"\n\n---------------------------------------------\n{text}"
    action.save()
