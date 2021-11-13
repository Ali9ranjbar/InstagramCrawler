from preprocess.PolarityNormalizer import PolarityNormalizer
import sys
import logging
import os
import json
import re
import string


class PersianPolarity:

    def __init__(self, logger):
        self.src_path = os.path.abspath(__file__)
        self.root_path = self.src_path.replace('PersianPolarity.py', '')  # get current loc. of Disk
        self.logger = logger
        self.AlphabetList = ['ا', 'ب', 'پ', 'ت', 'ت', 'ث', 'ج', 'چ', 'ح', 'خ', 'د', 'ذ', 'ر', 'ز', 'س', 'ش', 'ص', 'ض',
                             'ط',
                             'ظ',
                             'ع', 'غ', 'ف', 'ق', 'ک', 'گ', 'ل', 'م', 'ن', 'و', 'ه', 'ی', 'آ', 'آ', 'ئ', 'أ', 'ؤ', 'ژ',
                             'ي',
                             'پ',
                             'ۀ', 'ۀ', 'ة', 'ؤ', 'ي', 'ؤ', 'إ', 'أ', 'ء', ':,-)', '=)', ':)', '8)',
                             '🌴', '⭕', '😀', '💐', '😃', '😄', '🦖', '🌟', '🎂', '😁', '🎊', '📍', '😆', '😅', '🔪',
                             ':🤣',
                             '😂', '🙂', '❣', '🙃', '☘', '😉', '😊', '😇', '🥰', '😍', '🤩', '', '😘', '📘', '😗', '☺',
                             '😚',
                             '😙', '😋', '😛', '😜', '🤪', '💫', '🔔', '✅', '😝', '🤑', '🤗', '🤭', '🤫', '🖤', '🏽️',
                             '🏼️',
                             '🤔', '🤐', '🤨', '😐', '😑', '😶', '📥', '💣', '🍀', '🦋', '💣', '📌', '😏', '😒', '🙄',
                             '📆',
                             '😬', '🤥', '😌', '😔', '😪', '🤤', '🌒', '😴', '😷', '🤒', '🤕', '🤢', '🤮', '🤧', '🥵',
                             '🥶',
                             '🥴', '😵', '🤯', '🤠', '🦌', '🥳', '😎', '🤓', '🧐', '😕', '🏻', '🚨', '😟', '🙁', '☹',
                             '😮',
                             '😯',
                             '😲', '😳', '🥺', '😦', '😧', '🍒️', '🌶', '🍶', '😨', '😰', '😥', '😢', '😭', '😱', '😖',
                             '😣',
                             '😞', '😓', '😩', '😫', '💕', '😤', '😡', '😠', '🤬', '🏴', '🎥', '😈', '👿', '🔸', '🌸',
                             '💀',
                             '☠',
                             '💩', '🤡', '👹', '👺', '👻', '👽', '👾', '☔', '🤖', '😺', '😸', '😹', '😻', '😼', '😽',
                             '🙀',
                             '😿',
                             '😾', '💋', '👋', '🤚', '🖐', '💔', '✋', '🙈', '💚', '🖖', '👌', '✌', '🤞', '🤟', '🤘',
                             '🔥',
                             '🤙',
                             '👈', '👉', '👆', '🖕', '👇', '☝', '👍', '👎', '💜', '✊', '👊', '🤛', '🤜', '👏', '🙌',
                             '👐',
                             '🤲',
                             '🤝', '🙏', '✍', '💅', '🤳', '🔰', '💪', '🦵', '🦶', '💥', '👂', '👃', '🧠', '🍂', '🐥',
                             '🌸',
                             '🌺',
                             '🏾', '✨', '🦷', '🦴', '👀', '👁', '👅', '👄', '👶', '🧒', '👦', '👧', '🧑', '👱', '👨',
                             '🧔',
                             '👱',
                             '👨', '👨', '👨', '👨', '👩', '👱', '👩', '👩', '👩', '👩', '🧓', '♦', '👴', '👵', '🙍',
                             '🙍',
                             '🙍',
                             '🙎', '🙎', '🙎', '🙅', '🙅', '🙅', '🙆', '🙆', '🙆', '💁', '💁', '💁', '🙋', '🙋', '🙋',
                             '🙇',
                             '🙇', '🙇', '🤦', '🤦', '🤦', '🤷', '🤷', '🤷', '👨', '👩', '👨', '👩', '👨', '👩', '👨',
                             '👩',
                             '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '📲',
                             '👨',
                             '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👮', '👮', '👮', '🍆', '🕵', '🕵',
                             '🕵',
                             '💂', '💂', '💂', '👷', '👷', '👷', '🤴', '👸', '👳', '👳', '👳', '👲', '🧕', '🤵', '👰',
                             '🤰',
                             '🌹', '⬇', '♂', '❤', '🤱', '👼', '🎅', '🤶', '💙', '🐍', '🦸', '🦸', '🦹', '🦹', '✏', '♀',
                             '🦹',
                             '🧙', '🧙', '🧙', '🧚', '🧚', '🧚', '🧛', '🧛', '➿', '🧛', '🧜', '🧜', '🧜', '🧝', '🧝',
                             '🧝',
                             '🧞',
                             '🧞', '🧞', '🧟', '🧟', '🔴', '💆', '💆', '💆', '💇', '💇', '💇', '🚶', '🚶', '🚶', '🏃',
                             '🏃',
                             '🏃', '💃', '🕺', '🕴', '👯', '👯', '👯', '💢', '🏻', '🧖', '🧖', '🧘', '👭', '👫', '👬',
                             '💏',
                             '👨', '👩', '💑', '👨', '👩', '👪', '👨', '👨', '👨', '👨', '👨', '👨', '👨', '👨', '💙',
                             '💛',
                             '🔵', '⚫', '👨', '👨', '👩', '👩', '👩', '👩', '👩', '🔺', '👨', '👨', '👨', '👨', '👨',
                             '👩',
                             '👩',
                             '👩', '👩', '👩', '🗣', '👤', '👥', '👣', '🧳', '🌂', '☂', '🧵', '🧶', '👓', '🕶', '🥽',
                             '🥼',
                             '👔',
                             '👕', '👖', '🧣', '🧤', '🧥', '🧦', '👗', '👘', '👙', '👚', '👛', '👜', '👝', '🎒', '👞',
                             '👟',
                             '🥾', '🥿', '👠', '👡', '👢', '👑', '👒', '🎩', '🎓', '🧢', '⛑', '💄', '💍', '💼', '1',
                             '2',
                             '3',
                             '4', '5', '6', '7', '8', '9', '0', '۱', '۲', '۳', '۴', '۵', '۶', '۷', '۸', '۹', '۰', '!',
                             '@',
                             '#',
                             '$', '%', '^', '&', '*', '(', ')', ')', '_', '+', '=', '-', '|', '|', '}', '{', '[', ']',
                             ':',
                             ':',
                             '.', '<', '/', '?', '؟', '>', 'ٌ', '،', '\\', '!', '@', '#', '$', '%', '^', '&', '*', '(',
                             '_',
                             '-',
                             '+', '+', '|', ',', '|'
            , 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v', 'b', 'n', 'm', 'q', 'w', 'e', 'r', 't',
                             'y',
                             'u', 'i', 'o', 'p',
                             'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O',
                             'P',
                             'Z',
                             'X', 'C', 'V', 'B', 'N', 'M']
        self.POSETIVE_EMOJI = [':-)', '=)', ':)', '8)', ':]', '=]', '=>', '8-)', ':->', ':-]', ':”)', ':’)', ':-)',
                               '=)',
                               ':3',
                               ':>', ':ˆ)', ':-3', '=>', ':->', ':-V', '=v',
                               ':-1', 'ˆ', 'ˆ', 'ˆLˆ', 'ˆ)ˆ', ':*', ':-*', ';-)', ';)', ';-]', ';]', ';->', ';>', '%-}',
                               '<3',
                               ':-D', ':D', '=D', ':-P', '=3', 'xD', ':P', '=P',
                               'O.o', 'o.O', ':v', 'B)', 'B-)', 'B|', '8|', ':’-)', ':!', ':-X', '=*', ':-*', ':*',
                               '😀', '😃', '😄', '😁', '😆', '😅', '😳', '😎', '😺', '😸', '😹', '😻', '😼', '💋', '💪',
                               '👀', '👁', '👅', '👄', '👶', '🙋', '😏', '🤣', '❤', '😂', '🙂', '🙃', '😉', '😊', '😇',
                               '😍', '😘', '😗', '☺', '😚', '😙', '😋', '😛', '😜', '😝', '🙄', '👍', '💙', '🎀', '🌸',
                               '🌹',
                               '🌷'
                               ]
        self.NEGATIVE_EMOJI = ['- -', ':-(', ':(', ':[', ':-<', ':-[', '=(', ':-@', ':-&', ':-t', ':-z', ':<)', '}-(',
                               ':o',
                               ':O', ':-o', ':-O', ':-\\',
                               ':-/', ':-.', ':\\', ':’(', ':, (', ':’-(', ':, -(', ':˜(˜˜', ':˜-(',
                               '😟', '🙁', '😮', '😯', '😲', '😳', '😦', '😧', '😮', '😨', '😰', '😥', '😢', '😭', '😱',
                               '😖', '😣', '😞', '😓', '😩', '😫', '😤', '😡', '😠', '👿', '💀', '☠', '💩', '👎', '😑']
        self.EMOJI = [':-)', '=)', ':)', '8)', ':]', '=]', '=>', '8-)', ':->', ':-]', ':”)', ':’)', '=)', ':3', ':>',
                      ':ˆ)',
                      '=>', ':->', ':-V', '=v', ':-1', 'ˆ', 'ˆ', 'ˆLˆ', 'ˆ)ˆ', ':*', ':-*', ';-)', ';)', ';-]', ';]',
                      ';->',
                      ';>', '%-}', '<3', ':-D', ':D', '=D', ':-P', '=3', 'xD', ':P', '=P', 'O.o', 'o.O', ':v', 'B)',
                      'B-)',
                      'B|', '8|', ':’-)', ':!', ':-X', '=*', ':-*', ':*', '- -', ':-(', ':(', ':[', ':-<', ':-[', '=(',
                      ':-@', ':-&', ':-t', ':-z', ':<)', '}-(', ':o', ':O', ':-o', ':-O', ':-\\', ':-/', ':-.', ':\\',
                      ':’(',
                      ':, (', ':’-(', ':, -(', ':˜(˜˜', ':˜-(', '🌴', '⭕', '😀', '💐', '😃', '😄', '🦖', '🌟', '🎂',
                      '😁',
                      '🎊', '📍', '😆', '😅', '🔪', ':', '🤣', '😂', '🙂', '❣', '🙃', '☘', '😉', '😊', '٪', '😇', '🥰',
                      '😍',
                      '🤩', '😘', '📘', '😗', '☺', '😚', '😙', '😋', '😛', '😜', '🤪', '💫', '🔔', '✅', '😝', '🤑',
                      '🤗',
                      '🤭', '🤫', '🖤', '🏽️', '🏼️', '🤔', '🤐', '🤨', '😐', '😑', '😶', '📥', '💣', '🍀', '🦋', '💣',
                      '📌',
                      '😏', '😒', '🙄', '📆', '😬', '🤥', '😌', '😔', '😪', '🤤', '🌒', '😴', '😷', '🤒', '🤕', '🤢',
                      '🤮',
                      '🤧', '🥵', '🥶', '🥴', '😵', '🤯', '🤠', '🦌', '🥳', '😎', '🤓', '🧐', '😕', '🏻', '🚨', '😟',
                      '🙁',
                      '☹', '😮', '😯', '😲', '😳', '🥺', '😦', '😧', '🍒️', '🌶', '🍶', '😨', '😰', '😥', '😢', '😭',
                      '😱',
                      '😖', '😣', '😞', '😓', '😩', '😫', '💕', '😤', '😡', '😠', '🤬', '🏴', '🎥', '😈', '👿', '🔸',
                      '🌸',
                      '💀', '☠', '💩', '🤡', '👹', '👺', '👻', '👽', '👾', '☔', '🤖', '😺', '😸', '😹', '😻', '😼',
                      '😽',
                      '🙀', '😿', '😾', '💋', '👋', '🤚', '🖐', '💔', '✋', '🙈', '💚', '🖖', '👌', '✌', '🤞', '🤟',
                      '🤘',
                      '🔥', '🤙', '👈', '👉', '👆', '🖕', '👇', '☝', '👍', '👎', '💜', '✊', '👊', '🤛', '🤜', '👏',
                      '🙌',
                      '👐', '🤲', '🤝', '🙏', '✍', '💅', '🤳', '🔰', '💪', '🦵', '🦶', '💥', '👂', '👃', '🧠', '🍂',
                      '🐥',
                      '🌸', '🌺', '🏾', '✨', '🦷', '🦴', '👀', '👁', '👅', '👄', '👶', '🧒', '👦', '👧', '🧑', '👱',
                      '👨',
                      '🧔', '👱', '👨', '👨', '👨', '👨', '👩', '👱', '👩', '👩', '👩', '👩', '🧓', '♦', '👴', '👵',
                      '🙍',
                      '🙍', '🙍', '🙎', '🙎', '🙎', '🙅', '🙅', '🙅', '🙆', '🙆', '🙆', '💁', '💁', '💁', '🙋', '🙋',
                      '🙋',
                      '🙇', '🙇', '🙇', '🤦', '🤦', '🤦', '🤷', '🤷', '🤷', '👨', '👩', '👨', '👩', '👨', '👩', '👨',
                      '👩',
                      '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '📲', '👨',
                      '👩',
                      '👨', '👩', '👨', '👩', '👨', '👩', '👨', '👩', '👮', '👮', '👮', '🍆', '🕵', '🕵', '🕵', '💂',
                      '💂',
                      '💂', '👷', '👷', '👷', '🤴', '👸', '👳', '👳', '👳', '👲', '🧕', '🤵', '👰', '🤰', '🌹', '⬇',
                      '♂',
                      '❤', '🤱', '👼', '🎅', '🤶', '💙', '🐍', '🦸', '🦸', '🦹', '🦹', '✏', '♀', '🦹', '🧙', '🧙', '🧙',
                      '🧚', '🧚', '🧚', '🧛', '🧛', '➿', '🧛', '🧜', '🧜', '🧜', '🧝', '🧝', '🧝', '🧞', '🧞', '🧞',
                      '🧟',
                      '🧟', '🔴', '💆', '💆', '💆', '💇', '💇', '💇', '🚶', '🚶', '🚶', '🏃', '🏃', '🏃', '💃', '🕺',
                      '🕴',
                      '👯', '👯', '👯', '💢', '🏻', '🧖', '🧖', '🧘', '👭', '👫', '👬', '💏', '👨', '👩', '💑', '👨',
                      '👩',
                      '👪', '👨', '👨', '👨', '👨', '👨', '👨', '👨', '👨', '💙', '💛', '🔵', '🙊', '⚫', '👨', '👨',
                      '👩',
                      '👩',
                      '👩', '👩', '👩', '🔺', '👨', '👨', '👨', '👨', '👨', '👩', '👩', '👩', '👩', '👩', '🗣', '👤',
                      '👥',
                      '👣', '🧳', '🌂', '☂', '🧵', '🧶', '👓', '🕶', '🥽', '🥼', '👔', '👕', '👖', '🧣', '🧤', '🧥',
                      '🧦',
                      '👗', '👘', '👙', '👚', '👛', '👜', '👝', '🎒', '🌷', '👞', '👟', '🥾', '🥿', '👠', '👡', '👢',
                      '👑',
                      '👒',
                      '🎩', '🎓', '🧢', '⛑', '💄', '💍', '💼', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '۱',
                      '۲',
                      '۳', '۴', '۵', '۶', '۷', '۸', '۹', '۰', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', ')',
                      '_',
                      '+', '=', '-', '|', '|', '}', '{', '[', ']', ':', ':', ',', '.', '<', '/', '?', '؟', '>', '💙',
                      '🎀',
                      '🌸', '🌹']
        self.badword = ['دزد', 'اسکل', 'شرف', 'نمیده', 'حروم', 'آنتن', 'ضعیف', 'ضعیفه', 'افتضاح', 'کسکش', 'عوضی',
                        'مسدود',
                        'اشغال', 'ملت', 'ملتو', 'درد', 'دردمون', 'درد', 'گرون', 'گران', 'دزدا', 'دزدی', 'کون', 'ریدین',
                        'ریدی',
                        'سرعت', 'کمه', 'ظالم', 'کسخل', 'بیشرف', 'مرگ', 'مملکت', 'تیغ', 'قیمت', 'گرونه', 'کثیف',
                        'بیت المال',
                        'بیتالمال', 'جیب', 'زور', 'تف', 'عوضی', 'کثافت', 'خاک', 'بخور', 'قطع', 'بدترین', 'تخم', 'دروغ',
                        'گرون',
                        'مفت', 'گدا', 'خر', 'بیشرف', 'افتضاح', 'مسخر', 'شکایت', 'لعنت', 'چرت', 'بیشعور', 'ضعیف',
                        'سرکار', 'کوس',
                        'درست و حسابی', 'شارژ', 'تبلیغات', 'کلاه', 'کیر', 'مخ', 'گوه', 'اعصاب', 'کص', 'تعرفه', 'شعور',
                        'سرکار', 'مضخرف']
        self.goodword = ['منون', 'شکر', 'لایک', 'like', 'دمت', 'گرم', 'حال', 'تشکر', 'موفق', 'عالی', 'آفرین', 'افرین',
                         'مچکر',
                         'موفق', 'عزیز', 'خوش', 'العاده', 'سپاس', 'الله']

        self.Stopwords = []
        try:
            CLStopwords = list()
            with open(self.root_path+"resources/polarity_stopword.txt", 'r', encoding='utf-8') as file_reader:
                CLStopwords = file_reader.readlines()
            for word in CLStopwords:
                word = word.strip()
                self.Stopwords.append(word)
        except Exception as e:
            logger.info("EX: " + str(e))
        self.WordDict = []
        try:
            with open(self.root_path+"resources/polarity_dict.json", encoding='utf-8') as json_file:
                self.WordDict = json.load(json_file)
            for word in self.badword:
                Value = [0, 1]
                self.WordDict[word] = Value
            for word in self.goodword:
                Value = [1, 0]
                self.WordDict[word] = Value
        except Exception as e:
            logger.info("EX: " + str(e))


    def word_duplicat_remover(self, word):
        '''
              :param Token: Word
              :return: word that removed duplicate char
           '''
        if (word == ''):
            return ''
        charlist = list(word)
        return_word = ''
        for i in range(len(charlist) - 1):
            if charlist[i] != charlist[i + 1]:
                return_word = return_word + charlist[i]
        return_word = return_word + charlist[len(charlist) - 1]
        return return_word

    def mention_remover(self, sentance):
        '''
           :param Token: sentance
           :return: remove mention sentance text
        '''
        rsentence = ''
        charlist = list(sentance)
        mentionflag = False
        for i in range(len(charlist)):
            if charlist[i] == '@':
                mentionflag = True
            if charlist[i] == ' ':
                mentionflag = False
            if not mentionflag:
                rsentence = rsentence + charlist[i]
        return rsentence

    def persian_word_extractor(self, Sentance):
        '''
          :param Sentance
          :return: remove other unalphbet char
          '''

        WORD = re.compile(r'\w+')
        ESentance = list()
        for word in WORD.findall(Sentance):
            ExWord = ''
            EWord = list()
            Word = list(word)
            for Char in Word:
                if Char in self.AlphabetList:
                    ExWord = ExWord + Char
                else:
                    if ExWord != '':
                        EWord.append(ExWord)
                    ExWord = ''
            if ExWord != '':
                EWord.append(ExWord)
            ESentance.extend(EWord)
        ESentance = [x + ' ' for x in ESentance]
        ESentance = ''.join(ESentance)
        return ESentance

    def Polarity_detect(self, comment):
        word_count = 0
        sen = comment
        Normalizer = PolarityNormalizer(sen)
        sen = Normalizer.text
        Normsen = self.mention_remover(sen)
        Normsen = self.persian_word_extractor(Normsen)
        posetiveEmoji = [value for value in self.POSETIVE_EMOJI if value in sen]
        posetive_count = len(posetiveEmoji)
        negativemoji = [value for value in self.NEGATIVE_EMOJI if value in sen]
        negative_count = len(negativemoji)
        if (posetive_count > negative_count):
            sent_score = 1
        elif (posetive_count < negative_count):
            sent_score = -1
        else:
            Words = re.sub('[' + string.punctuation + '،' + '؛' ']', ' ', Normsen).split()
            sent_score = 0
            CListWord = list()
            for word in Words:
                word = self.word_duplicat_remover(word)
                for emoji in self.EMOJI:
                    if emoji in word:
                        word = word.replace(emoji, '')
                        CListWord.append(emoji)
                CListWord.append(word)
            Words = CListWord
            NormSen = ''
            for word in Words:
                word = self.word_duplicat_remover(word)
                if word not in self.Stopwords:
                    if word in self.WordDict:
                        if self.WordDict[word][0] > 0 or self.WordDict[word][1] > 0:
                            word_count = word_count + 1
                            word_sum = self.WordDict[word][0] * 0.78 + self.WordDict[word][1] * 0.22
                            word_score = (self.WordDict[word][0] * 0.22 - self.WordDict[word][1] * 0.78) / word_sum
                            sent_score = sent_score + word_score
                        NormSen += word + ' '

            BadwordCount = [value for value in self.badword if value in sen]
            GoodwordCount = [value for value in self.goodword if value in sen]
            sent_score = sent_score - (float(len(BadwordCount)))
            word_count = word_count + len(BadwordCount)
            sent_score = sent_score + (float(len(GoodwordCount)))
            word_count = word_count + len(GoodwordCount)
            try:
                sent_score = sent_score / word_count
            except:
                sent_score = 0
        # Tag = 0
        Tag = 'z'
        sent_score = sent_score - 0.055
        if sent_score >= 0.18:
            # Tag = 2
            Tag = 'p'
        elif sent_score < -0.1:
            # Tag = -2
            Tag = 'n'
        else:
            # Tag = 0
            Tag = 'z'
            if sent_score < -0.02:
                # Tag = -1
                Tag = 'n'
            elif sent_score > 0.05:
                # Tag = 1
                Tag = 'p'
        result = {'lable': Tag, 'score': sent_score}
        return result


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        format='[%(asctime)s] %(filename)s %(lineno)d %(levelname)s %(name)s %(message)s', level=logging.INFO)
    polarity = PersianPolarity(logging)
    label = polarity.Polarity_detect('سلام من خالم خوبه خوبه رفقا')
    print(label)
