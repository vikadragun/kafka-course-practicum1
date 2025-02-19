class Message:
    def __init__(self, key, text): # конструктор класса
        self.key = key
        self.text = text

    def create_message(self): # приведение сообщения к типу string
        return f'{self.key}:{self.text}'
