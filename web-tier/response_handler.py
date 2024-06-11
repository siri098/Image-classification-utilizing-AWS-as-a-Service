import sqlite3

from gevent import sleep


class ResponseHandler:
    WAIT_STRING = "WAIT"

    def __init__(self) -> None:
        self.con = sqlite3.connect('responses.db')
        self._create_responses_table()
        # self._print_table()

    def _print_table(self) -> None:
        cur = self.con.cursor()
        cur.execute('SELECT * FROM responses')
        print(cur.fetchall())
    
    def _create_responses_table(self) -> None:
        cur = self.con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS responses (messageId TEXT PRIMARY KEY, response TEXT)')
        self.con.commit()

    def add_response(self, messageId: str, result: str = WAIT_STRING) -> None:
        print("Adding response for message", messageId)
        cur = self.con.cursor()
        cur.execute('INSERT OR REPLACE INTO responses VALUES (?, ?)', (messageId, result))
        self.con.commit()

    def _delete_response(self, messageId: str) -> None:
        print("Deleting response for message", messageId)
        cur = self.con.cursor()
        cur.execute('DELETE FROM responses WHERE messageId=?', (messageId,))
        self.con.commit()
    
    def get_response(self, messageId: str, max_timeout = 600) -> str:
        print("Getting response for message", messageId)
        cur = self.con.cursor()
        result = self.WAIT_STRING
        count = max_timeout * 2
        while result == self.WAIT_STRING and count > 0:
            cur.execute('SELECT response FROM responses WHERE messageId=?', (messageId,))
            result = cur.fetchone()
            count -= 1
            sleep(0.5)
            if result is not None:
                result = result[0]
        if result is not None:
            self._delete_response(messageId)
        print("Got response for message", messageId)
        if result == self.WAIT_STRING:
            return None
        return result

