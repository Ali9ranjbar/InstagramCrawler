
# http://www.fileformat.info/info/unicode/block/arabic/list.htm

import re
# -*- coding: utf-8 -*-
# ==============================================================================
import json
# ==============================================================================
# %% functions
# ==============================================================================
class PolarityNormalizer:
     def __init__(self,text ):
          self.text=text

     @property
     def persian_normalize(self):
          normalized_text = []
          if  self.text is None:
               return ""
          # ================ Numbers ======================
          # Persian Numbers
          FA_D0 = '\u06F0'
          FA_D1 = '\u06F1'
          FA_D2 = '\u06F2'
          FA_D3 = '\u06F3'
          FA_D4 = '\u06F4'
          FA_D5 = '\u06F5'
          FA_D6 = '\u06F6'
          FA_D7 = '\u06F7'
          FA_D8 = '\u06F8'
          FA_D9 = '\u06F9'
          # Arabic Numbers
          Ar_D0 = '\u0660'
          Ar_D1 = '\u0661'
          Ar_D2 = '\u0662'
          Ar_D3 = '\u0663'
          Ar_D4 = '\u0664'
          Ar_D5 = '\u0665'
          Ar_D6 = '\u0666'
          Ar_D7 = '\u0667'
          Ar_D8 = '\u0668'
          Ar_D9 = '\u0669'
          # English Numbers
          EN_D0 = '\u0030'
          EN_D1 = '\u0031'
          EN_D2 = '\u0032'
          EN_D3 = '\u0033'
          EN_D4 = '\u0034'
          EN_D5 = '\u0035'
          EN_D6 = '\u0036'
          EN_D7 = '\u0037'
          EN_D8 = '\u0038'
          EN_D9 = '\u0039'
          # Mosavet Charecters
          # ---------------------  DAL  -----------------------------------
          ARABIC_LETTER_DAL = u'\u062F'            #   د
          ARABIC_LETTER_DDAL = u'\u0688'             #    ڈ
          ARABIC_LETTER_DAL_WITH_RING = u'\u0689'      #   ډ
          ARABIC_LETTER_DAL_WITH_DOT_BELOW = u'\u068A'   #  ڊ
          ARABIC_LETTER_DAL_WITH_DOT_BELOW_AND_SMALL_TAH = u'\u068B'     #  ڋ
          ARABIC_LETTER_DAHAL = u'\u068C'     #  ڌ
          ARABIC_LETTER_DDAHAL = u'\u068D'     #  ڍ
          ARABIC_LETTER_DUL = u'\u068E'          #   ڎ
          ARABIC_LETTER_DAL_WITH_THREE_DOTS_ABOVE_DOWNWARDS = u'\u068F'      #    ڏ
          ARABIC_LETTER_DAL_WITH_FOUR_DOTS_ABOVE = u'\u0690'    #    ڐ
          # -----------------------  REH ------------------------------------
          ARABIC_LETTER_REH = u'\u0631'              #  ر
          ARABIC_LETTER_RREH = u'\u0691'              #  ڑ
          ARABIC_LETTER_REH_WITH_SMALL_V = 'u\u0692'   #  ڒ
          ARABIC_LETTER_REH_WITH_RING = u'\u0693'       #   ړ
          ARABIC_LETTER_REH_WITH_DOT_BELOW = u'\u0694'            #    ڔ
          ARABIC_LETTER_REH_WITH_SMALL_V_BELOW = u'\u0695'          #    ڕ
          ARABIC_LETTER_REH_WITH_DOT_BELOW_AND_DOT_ABOVE = u'\u0696'  #    ږ
          ARABIC_LETTER_REH_WITH_TWO_DOTS_ABOVE = u'\u0697'  #  ڗ
          # ----------------------  SEEN  -----------------------------------
          ARABIC_LETTER_SEEN = u'\u0633'              #     س
          ARABIC_LETTER_SEEN_WITH_THREE_DOTS_BELOW = u'\u069B'           #    ڛ
          ARABIC_LETTER_SEEN_WITH_DOT_BELOW_AND_DOT_ABOVE = u'\u069A'   #    ښ
          # ---------------------  YEH  -----------------------------
          DOTLESS_YEH2 = u'\u06cc'                              # ی  == persian
          DottedYEH = u'\u064A'                               # ي
          DOTLESS_YEH1 = u'\u0649'                             # ى
          ARABIC_LETTER_YEH_WITH_HAMZA_ABOVE = u'\u0626'        # ئ
          ARABIC_LETTER_KASHMIRI_YEH=u'\u0620'                   #  ؠ
          ARABIC_LETTER_FARSI_YEH_WITH_INVERTED_V = u'\u063D'      # ؽ
          ARABIC_LETTER_FARSI_YEH_WITH_TWO_DOTS_ABOVE = u'\u063E'     # ؾ
          ARABIC_LETTER_FARSI_YEH_WITH_THREE_DOTS_ABOVE = u'\u063F'     # ؿ
          ARABIC_LETTER_HIGH_HAMZA_YEH = u'\u0678'                      # ٸ
          # ----------------------  KAF  ----------------------------
          ARABIC_KAF = u'\u0643'                                  # ك
          ARABIC_KEHEH = u'\u06A9'                                 # ک  persian
          ARABIC_LETTER_KEHEH_WITH_TWO_DOTS_ABOVE =u'\u063B'         # ػ
          ARABIC_LETTER_KEHEH_WITH_THREE_DOTS_BELOW = u'\u063C'        #  ؼ
          ARABIC_LETTER_SWASH_KAF = u'\u06AA'                           # ڪ
          ARABIC_LETTER_KAF_WITH_RING = u'\u06AB'                         # ګ
          ARABIC_LETTER_KAF_WITH_DOT_ABOVE = 'u\u06AC'                      # ڬ
          ARABIC_LETTER_NG = u'\u06AD'                                       #  ڭ
          ARABIC_LETTER_KAF_WITH_THREE_DOTS_BELOW = 'u\u06AE'                 #   ڮ
          # ---------------------  GAF -------------------------------
          ARABIC_LETTER_GAF = u'\u06AF'                   #  گ persian
          ARABIC_LETTER_GAF_WITH_RING = u'\u06B0'          # ڰ
          ARABIC_LETTER_NGOEH = u'\u06B1'                    #  ڱ
          ARABIC_LETTER_GAF_WITH_TWO_DOTS_BELOW = 'u\u06B2'    #   ڲ
          ARABIC_LETTER_GUEH = 'u\u06B3'                         #   ڳ
          ARABIC_LETTER_GAF_WITH_THREE_DOTS_ABOVE = u'\u06B4'     #    ڴ
          # ---------------------  LAM  ------------------------------
          ARABIC_LETTER_LAM = u'\u0644'                    #  ل
          ARABIC_LETTER_LAM_WITH_SMALL_V = u'\u06B5'         #  ڵ
          ARABIC_LETTER_LAM_WITH_DOT_ABOVE = u'\u06B6'         #   ڶ
          ARABIC_LETTER_LAM_WITH_THREE_DOTS_ABOVE = u'\u06B7'    #   ڷ
          ARABIC_LETTER_LAM_WITH_THREE_DOTS_BELOW = u'\u06B8'      #   ڸ
          # --------------------  BEH  ---------------------
          ARABIC_LETTER_DOTLESS_BEH = u'\u066E'           #   ٮ
          ARABIC_LETTER_BEH = u'\u0628'        #     ب
          # -------------------- ALEF ---------------------------------
          ARABIC_LETTER_ALEF = u'\u0627'                     #  ا
          ARABIC_LETTER_ALEF_WITH_HAMZA_ABOVE =u'\u0623'   # أ
          ARABIC_LETTER_ALEF_WITH_MADDA_ABOVE = u'\u0622'     #  آ
          ARABIC_LETTER_ALEF_WITH_HAMZA_ABOVE = u'\u0623'      #  أ
          RABIC_LETTER_ALEF_WITH_HAMZA_BELOW = u'\u0625'        #   إ
          ARABICـLETTERـALEFـWASLA =u'\u0671'                    #     ٱ
          ARABIC_LETTER_ALEF_WITH_WAVY_HAMZA_ABOVE = u'\u0672'    #     ٲ
          ARABIC_LETTER_ALEF_WITH_WAVY_HAMZA_BELOW = u'\u0673'     #      ٳ
          # --------------------- NOON  --------------------------------------------
          ARABIC_LETTER_NOON = u'\u0646'    #  ن
          ARABIC_LETTER_NOON_WITH_DOT_BELOW = u'\u06B9'    #  ڹ
          ARABIC_LETTER_NOON_GHUNNA = u'\u06BA'             #  ں
          ARABIC_LETTER_RNOON = u'\u06BB'             #  ڻ
          ARABIC_LETTER_NOON_WITH_RING = u'\u06BC'     #   ڼ
          ARABIC_LETTER_NOON_WITH_THREE_DOTS_ABOVE = u'\u06BD'  #    ڽ
          # ----------------------  ح HAH  --------------------
          ARABIC_LETTER_HAH = u'\u062D'                            #   ح
          ARABIC_LETTER_HAH_WITH_HAMZA_ABOVE = u'\u0681'            #    ځ
          ARABIC_LETTER_HAH_WITH_TWO_DOTS_VERTICAL_ABOVE = u'\u0682'  #    ڂ
          # --------------------  WAW ---------------------------------
          ARABIC_LETTER_WAW = u'\u0648'                   #    و
          ARABIC_LETTER_WAW_WITH_HAMZA_ABOVE = u'\u0624'  # ؤ
          ARABIC_LETTER_HIGH_HAMZA_WAW = u'\u0676'           #     ٶ
          ARABIC_LETTER_WAW_WITH_RING = u'\u06C4'              #    ۄ
          ARABIC_LETTER_WAW_WITH_TWO_DOTS_ABOVE = u'\u06CA'       #    ۊ
          ARABIC_LETTER_WAW_WITH_DOT_ABOVE = u'\u06CF'              #    ۏ
          # =============================================
          ARABIC_LETTER_HAMZA =u'\u0621'   # ء
          # ---------------------  HEH - TEH_MARBUTU  ------------------------------
          TEH_MARBUTA = u'\u0629'        #TEH_MARBUTA: ة
          HEH = u'\u0647'                #HEH=> ه
          # --------------------  ERAB -----------------------------------------
          TATWEEL = u'\u0640'           # ـ
          FATHATAN = u'\u064B'             # ً
          DAMMATAN = u'\u064C'              # ٌ
          KASRATAN = u'\u064D'                    # ٍ
          FATHA = u'\u064E'                   # َ
          DAMMA = u'\u064F'                    # ُ
          ARABIC_KASRA = u'\u0650'            #          ِ
          arial_unic = u'\u0650'                     # ِ
          SHADDA = u'\u0651'                     # ّ
          SUKUN = u'\u0652'                       #  ْ
          ARABIC_SMALL_HIGH_ZAIN = u'\u0617'      #          ؗ
          ARABIC_SMALL_HIGH_LIGATURE_ALEF_WITH_LAM_WITH_YEH = u'\u0616'   #  ؖ
          ARABIC_SMALL_HIGH_TAH = u'\u0615'             #   ؕ
          ARABIC_SIGN_TAKHALLUS = u'\u0614'              #   ؔ
          ARABIC_SIGN_RADI_ALLAHOU_ANHU = u'\u0613'       #   ؓ
          ARABIC_SIGN_RAHMATULLAH_ALAYHE = u'\u0612'         #    ؒ
          ARABIC_SIGN_ALAYHE_SSALLAM = u'\u0611'              #    ؑ
          ARABIC_SIGN_SALLALLAHOU_ALAYHE_WASSALLAM = u'\u0610'   #       ؐ
          ARABIC_SIGN_MISRA =u'\u060F'             #  ؏
          ARABIC_POETIC_VERSE_SIGN = u'\u060E'      #  ؎
          ARABIC_DATE_SEPARATOR = u'\u060D'  #  ؍
          AFGHANI_SIGN = u'\u060B'            #  ؋
          ARABIC_RAY = u'\u0608'                #  ؈
          ARABIC_NUMBER_MARK_ABOVE = 'u\u0605'    #  ؅
          ARABIC_SIGN_SAMVAT = 'u\0604'            #  ؄
          ARABIC_SIGN_SAFHA = u'\0603'              #  ؃
          ARABIC_FOOTNOTE_MARKER = u'\u0602'          #   ؂
          ARABIC_SIGN_SANAH = u'\u0601'           # ؁
          ARABIC_NUMBER_SIGN = u'\u0600'         #  ؀
          ARABIC_SMALL_FATHA = u'\u0618'          #   ؘ
          ARABIC_HAMZA_ABOVE = u'\u0654'           #  ٔ
          ARABIC_HAMZA_BELOW = u'\u0655'            # ٖ  ٕ
          ARABIC_SUBSCRIPT_ALEF = u'\u0656'          #
          ARABIC_INVERTED_DAMMA = u'\u0657'            #   ٗ
          ARABIC_MARK_NOON_GHUNNA = u'\u0658'                          #    ٘
          ARABIC_VOWEL_SIGN_SMALL_V_ABOVE = u'\u065A'           #   ٚ
          ARABIC_VOWEL_SIGN_INVERTED_SMALL_V_ABOVE = u'\u065B'    #	ٛ
          ARABIC_VOWEL_SIGN_DOT_BELOW = u'\u065C'      #    ٜ
          ARABIC_REVERSED_DAMMA = u'\u065D'                        #    ٝ
          ARABIC_FATHA_WITH_TWO_DOTS = u'\u065E'                    #    ٞ
          ARABIC_WAVY_HAMZA_BELOW = u'\u065F'                        #     ٟ
          ARABIC_LETTER_SUPERSCRIPT_ALEF = u'\u0670'         # ٰ
          ARABIC_LETTER_HIGH_HAMZA_ALEF = u'\u0674'             #  ٴ
          ARABIC_FULL_STOP = 'u\u06D4'                             #    ۔
          ARABIC_SMALL_LOW_MEEM = u'\u06ED'          #  ۭ
          ARABIC_EMPTY_CENTRE_HIGH_STOP = u'\u06EB'         #   ۫
          ARABIC_SMALL_HIGH_NOON = u'\u06E8'                     # ۨ
          ARABIC_SMALL_HIGH_YEH = u'\u06E7'                       #    ۧ
          ARABIC_SMALL_WAW = u'\u06E5'               #     ۥ
          ARABIC_SMALL_HIGH_MADDA = u'\u06E4'           #     ۤ
          ARABIC_SMALL_LOW_SEEN = u'\u06E3'                #     ۣ
          ARABIC_SMALL_HIGH_MEEM_ISOLATED_FORM = u'\u06E2'            #    ۢ
          ARABIC_SMALL_HIGH_DOTLESS_HEAD_OF_KHAH = u'\u06E1'             #   ۡ
          ARABIC_SMALL_HIGH_UPRIGHT_RECTANGULAR_ZERO = u'\u06E0'           #   ۠
          ARABIC_SMALL_HIGH_THREE_DOTS = u'\u06DB'                            #  ۛ
          ARABIC_SMALL_HIGH_MEEM_INITIAL_FORM = u'\u06D8'      #    ۘ
          ARABIC_SMALL_HIGH_LIGATURE_QAF_WITH_LAM_WITH_ALEF_MAKSURA = u'\u06D7'          #    ۗ
          ARABIC_SMALL_HIGH_LIGATURE_SAD_WITH_LAM_WITH_ALEF_MAKSURA = u'\u06D6'            #   ۖ
          ARABIC_SHADDA = u'\u0651'         #   ّ

          ARABIC_SMALL_HIGH_JEEM = u'\u06DA'      #    ۚ

          ARABIC_EMPTY_CENTRE_LOW_STOP = u'\u06EA'        # ۪

          ARABIC_SMALL_HIGH_LAM_ALEF = u'\u06D9'
                                                  #    ۙ
# ------------------- SPACE  -----------------------------------------------
          EN_QUAD = u'\u2000'    #SPACE
          EM_QUAD = u'\u2001'     #SPACE
          EN_SPACE = u'\u2002' #     #SPACE
          EM_SPACE = u'\u2003'  #       #SPACE
          THREE_PER_EM_SPACE = u'\u2004'     #SPACE
          FOUR_PER_EM_SPACE = u'\u2005'          #SPACE
          SIX_PER_EM_SPACE = u'\u2006'               #SPACE
          FIGURE_SPACE = u'\u2007'                    #SPACE
          THIN_SPACE = u'\u2009'                       #SPACE
          NARROW_NO_BREAK_SPACE = u'\u202F'              #SPACE
          MEDIUM_MATHEMATICAL_SPACE = u'\u205F'           #SPACE

          PUNCTUATION_SPACE = u'\u2008'       # نیم فاصله
          HAIR_SPACE = u'\u200A'               # نیم فاصله
          ZERO_WIDTH_NON_JOINER = u'\u200C'     # نیم فاصله

# ============== REMOVE ===========================
          ZERO_WIDTH_SPACE = u'\u200B'     #  چسبان
          ZERO_WIDTH_JOINER = u'\u200D'      #  حذف میکنم    چسبان
# ============================================
          LEFT_TO_RIGHT_MARK = u'\u200E'
          RIGHT_TO_LEFT_MARK = u'\u200F'
          LEFT_TO_RIGHT_OVERRIDE = u'\u202D'
          RIGHT_TO_LEFT_OVERRIDE = u'\u202E'
          POP_DIRECTIONAL_FORMATTING = u'\u202C'
 # ------------------- HYPHEN -   ----------------------------------------------
          HYPHEN =u'\u2010'
          NON_BREAKING_HYPHEN = u'\u2011'
# ------------------- DASH  –  ----------------------------------------------
          FIGURE_DASH = u'\u2012'
          EN_DASH = u'\u2013'
          EM_DASH = u'\u2014'
# -========================================  replace char in all text ===========================================
#           self.text = self.text.replace('“', '"')     #جایگزین کردن تمام  “ با "
#           self.text = self.text.replace('”', '"') #جایگزین کردن تمام  ” با "
          # text = self.text.replace('ىء', 'ء')#جایگزین کردن تمام  ىء با ء

          for c in self.text:
#========= evaluate end char of each word for  TEH_MARBUTA and replace with HEH ======================
                    if c == TEH_MARBUTA:    #if c == item[-1] and c == HEH:
                         normalized_text.append(HEH)
                    # ----------------------------- DAL  -----------------------------------------------------
                    elif c == ARABIC_LETTER_DDAL or c == ARABIC_LETTER_DAL_WITH_RING \
                              or c == ARABIC_LETTER_DAL_WITH_DOT_BELOW \
                              or c == ARABIC_LETTER_DAL_WITH_DOT_BELOW_AND_SMALL_TAH \
                              or c == ARABIC_LETTER_DAHAL or c == ARABIC_LETTER_DDAHAL \
                              or c == ARABIC_LETTER_DUL or c == ARABIC_LETTER_DAL_WITH_THREE_DOTS_ABOVE_DOWNWARDS \
                              or c == ARABIC_LETTER_DAL_WITH_THREE_DOTS_ABOVE_DOWNWARDS \
                              or c == ARABIC_LETTER_DAL_WITH_FOUR_DOTS_ABOVE:
                         normalized_text.append(ARABIC_LETTER_DAL)
                    # ----------------------------- REH -------------------------------------------------------
                    elif c == ARABIC_LETTER_RREH or c == ARABIC_LETTER_REH_WITH_SMALL_V \
                              or c == ARABIC_LETTER_REH_WITH_RING or c == ARABIC_LETTER_REH_WITH_DOT_BELOW \
                              or c == ARABIC_LETTER_REH_WITH_SMALL_V_BELOW or c == ARABIC_LETTER_REH_WITH_DOT_BELOW_AND_DOT_ABOVE \
                              or c == ARABIC_LETTER_REH_WITH_TWO_DOTS_ABOVE:
                              normalized_text.append(ARABIC_LETTER_REH)
                    # ------------------------------ SEEN   --------------------------------------------------
                    elif c == ARABIC_LETTER_SEEN_WITH_THREE_DOTS_BELOW \
                              or c == ARABIC_LETTER_SEEN_WITH_DOT_BELOW_AND_DOT_ABOVE:
                              normalized_text.append(ARABIC_LETTER_SEEN)
                    # ---------------------------- YEH-------------------------------------------------------
                    elif c == ARABIC_LETTER_YEH_WITH_HAMZA_ABOVE or c== DottedYEH or c == DOTLESS_YEH1 \
                             or c == ARABIC_LETTER_KASHMIRI_YEH \
                             or c == ARABIC_LETTER_FARSI_YEH_WITH_INVERTED_V \
                             or c == ARABIC_LETTER_FARSI_YEH_WITH_TWO_DOTS_ABOVE \
                             or c == ARABIC_LETTER_FARSI_YEH_WITH_THREE_DOTS_ABOVE \
                             or c == ARABIC_LETTER_HIGH_HAMZA_YEH:
                            normalized_text.append(DOTLESS_YEH2)
                    # ----------------------------_KAF--------------------------------------------------------
                    elif c == ARABIC_KAF \
                             or c == ARABIC_LETTER_KEHEH_WITH_TWO_DOTS_ABOVE \
                             or c == ARABIC_LETTER_KEHEH_WITH_THREE_DOTS_BELOW \
                             or c == ARABIC_LETTER_SWASH_KAF \
                             or c == ARABIC_LETTER_KAF_WITH_RING \
                             or c == ARABIC_LETTER_KAF_WITH_DOT_ABOVE \
                             or c == ARABIC_LETTER_NG \
                             or c == ARABIC_LETTER_KAF_WITH_THREE_DOTS_BELOW:
                           normalized_text.append(ARABIC_KEHEH)
                    # ----------------------------- GAF ------------------------------------------------------
                    elif c == ARABIC_LETTER_GAF_WITH_RING \
                              or c == ARABIC_LETTER_NGOEH \
                              or c == ARABIC_LETTER_GAF_WITH_TWO_DOTS_BELOW \
                              or c == ARABIC_LETTER_GUEH \
                              or c == ARABIC_LETTER_GAF_WITH_THREE_DOTS_ABOVE:  # print("c=", "ARABIC_GAF Replace with ARABIC_KAF: ",ARABIC_KAF)
                              normalized_text.append(ARABIC_LETTER_GAF)
                    # ----------------------------- LAM ------------------------------------------------------
                    elif c == ARABIC_LETTER_LAM_WITH_SMALL_V \
                              or c == ARABIC_LETTER_LAM_WITH_DOT_ABOVE \
                              or c == ARABIC_LETTER_LAM_WITH_THREE_DOTS_ABOVE \
                              or c == ARABIC_LETTER_LAM_WITH_THREE_DOTS_BELOW:
                              normalized_text.append(ARABIC_LETTER_LAM)
                    # ------------------------------- BEH ----------------------------------------------------
                    elif c == ARABIC_LETTER_DOTLESS_BEH:
                              normalized_text.append(ARABIC_LETTER_BEH)
                     # ----------------------------- ALEF -----------------------------------------------------
                    elif c == ARABIC_LETTER_ALEF_WITH_HAMZA_ABOVE or c == ARABIC_LETTER_ALEF_WITH_MADDA_ABOVE \
                             or c == ARABIC_LETTER_ALEF_WITH_HAMZA_ABOVE \
                             or c == RABIC_LETTER_ALEF_WITH_HAMZA_BELOW \
                             or c == ARABICـLETTERـALEFـWASLA \
                             or c == ARABIC_LETTER_ALEF_WITH_WAVY_HAMZA_ABOVE \
                             or c == ARABIC_LETTER_ALEF_WITH_WAVY_HAMZA_BELOW:
                             normalized_text.append(ARABIC_LETTER_ALEF)
                    # ----------------------------- NOON -----------------------------------------------------
                    elif c == ARABIC_LETTER_NOON_WITH_DOT_BELOW \
                             or c == ARABIC_LETTER_NOON_GHUNNA \
                            or c == ARABIC_LETTER_RNOON or c == ARABIC_LETTER_NOON_WITH_RING \
                            or c == ARABIC_LETTER_NOON_WITH_THREE_DOTS_ABOVE:
                            normalized_text.append(ARABIC_LETTER_NOON)
                    # ---------------------------- HAH  -------------------------------------------------------
                    elif c == ARABIC_LETTER_HAH_WITH_HAMZA_ABOVE or c == ARABIC_LETTER_HAH_WITH_TWO_DOTS_VERTICAL_ABOVE:
                            normalized_text.append(ARABIC_LETTER_HAH)

                    # ===================================================================
                    elif c == EM_QUAD or c == EN_SPACE or c == EM_SPACE  \
                           or c== THREE_PER_EM_SPACE or c == FOUR_PER_EM_SPACE \
                           or c == SIX_PER_EM_SPACE or c== FIGURE_SPACE \
                           or c==THIN_SPACE or c== NARROW_NO_BREAK_SPACE or c == MEDIUM_MATHEMATICAL_SPACE :
                           normalized_text.append(EN_QUAD)
                    # --------------------------- Half-space -------------------------------------------------
                    elif c == HAIR_SPACE  or c == ZERO_WIDTH_NON_JOINER:
                           normalized_text.append(PUNCTUATION_SPACE)
                     # ---------------------------- DASH  -----------------------------------------------------
                    elif c == FIGURE_DASH or c == EM_DASH:
                           normalized_text.append(EN_DASH)
                     # ---------------------------- HYPHEN ----------------------------------------------------
                    elif c == NON_BREAKING_HYPHEN:
                           normalized_text.append(HYPHEN)
                     # ---------------------------- WAW -------------------------------------------------------
                    elif c == ARABIC_LETTER_WAW_WITH_HAMZA_ABOVE or c== ARABIC_LETTER_HIGH_HAMZA_WAW \
                                    or c == ARABIC_LETTER_WAW_WITH_RING \
                                    or c == ARABIC_LETTER_WAW_WITH_TWO_DOTS_ABOVE \
                                    or c == ARABIC_LETTER_WAW_WITH_DOT_ABOVE:
                                    normalized_text.append(ARABIC_LETTER_WAW)
                    # ---------------------------- Evaluate Ereb  AND Skip -----------------------------------
                    elif c == TATWEEL or c == FATHATAN or c == DAMMATAN or c == KASRATAN \
                                    or c == FATHA or c == DAMMA or c == ARABIC_KASRA or c == arial_unic \
                                    or c == SHADDA or c == SUKUN or c == ARABIC_HAMZA_ABOVE or c == ARABIC_HAMZA_BELOW \
                                    or c == ARABIC_SUBSCRIPT_ALEF or c == ARABIC_INVERTED_DAMMA or c == ARABIC_MARK_NOON_GHUNNA \
                                    or c == ARABIC_VOWEL_SIGN_SMALL_V_ABOVE or c == ARABIC_VOWEL_SIGN_INVERTED_SMALL_V_ABOVE \
                                    or c == ARABIC_VOWEL_SIGN_DOT_BELOW or c == ARABIC_REVERSED_DAMMA or c == ARABIC_FATHA_WITH_TWO_DOTS \
                                    or c == ARABIC_WAVY_HAMZA_BELOW or c == ARABIC_LETTER_SUPERSCRIPT_ALEF or c == ARABIC_LETTER_HIGH_HAMZA_ALEF \
                                    or c == ARABIC_FULL_STOP or c == ARABIC_SMALL_LOW_MEEM or c == ARABIC_EMPTY_CENTRE_HIGH_STOP \
                                    or c == ARABIC_SMALL_HIGH_NOON or c == ARABIC_SMALL_HIGH_YEH or c == ARABIC_SMALL_WAW or c == ARABIC_SMALL_HIGH_MADDA \
                                    or c == ARABIC_SMALL_LOW_SEEN or c == ARABIC_SMALL_HIGH_MEEM_ISOLATED_FORM or c == ARABIC_SMALL_HIGH_DOTLESS_HEAD_OF_KHAH \
                                    or c == ARABIC_SMALL_HIGH_UPRIGHT_RECTANGULAR_ZERO or c == ARABIC_SMALL_HIGH_THREE_DOTS or c == ARABIC_SMALL_HIGH_MEEM_INITIAL_FORM \
                                    or c == ARABIC_SMALL_HIGH_LIGATURE_QAF_WITH_LAM_WITH_ALEF_MAKSURA or c == ARABIC_SMALL_HIGH_LIGATURE_SAD_WITH_LAM_WITH_ALEF_MAKSURA \
                                    or c == ARABIC_SHADDA or c == ARABIC_SMALL_HIGH_JEEM or c== ARABIC_EMPTY_CENTRE_LOW_STOP or c == ARABIC_SMALL_HIGH_LAM_ALEF\
                                    or c == ARABIC_SMALL_FATHA  or c == ARABIC_SMALL_HIGH_ZAIN  or c== ARABIC_SMALL_HIGH_LIGATURE_ALEF_WITH_LAM_WITH_YEH \
                                    or c == ARABIC_SMALL_HIGH_TAH  or c == ARABIC_SIGN_TAKHALLUS or c == ARABIC_SIGN_RADI_ALLAHOU_ANHU  or c == ARABIC_SIGN_RAHMATULLAH_ALAYHE \
                                    or c == ARABIC_SIGN_ALAYHE_SSALLAM  or c == ARABIC_SIGN_SALLALLAHOU_ALAYHE_WASSALLAM  or c == ARABIC_SIGN_MISRA \
                                    or c == ARABIC_POETIC_VERSE_SIGN  or c== ARABIC_DATE_SEPARATOR  or c ==  AFGHANI_SIGN  or c == ARABIC_RAY or c == ARABIC_NUMBER_MARK_ABOVE \
                                    or c == ARABIC_SIGN_SAMVAT  or c == ARABIC_SIGN_SAFHA  or c == ARABIC_FOOTNOTE_MARKER or c == ARABIC_SIGN_SANAH  or c == ARABIC_NUMBER_SIGN  \
                                    or c == ZERO_WIDTH_SPACE or c == ZERO_WIDTH_JOINER or c == LEFT_TO_RIGHT_OVERRIDE or c == RIGHT_TO_LEFT_OVERRIDE  \
                                    or c == POP_DIRECTIONAL_FORMATTING \
                                    or c == LEFT_TO_RIGHT_MARK or  c == RIGHT_TO_LEFT_MARK :
                              continue

                    # -----------------------convert English_Arabic_numbers to Persian_numbers ----------
                    elif c == Ar_D0 or c == EN_D0:
                         #print(c)
                         normalized_text.append(c.replace(c,FA_D0))
                    elif c == Ar_D1 or c == EN_D1:
                         normalized_text.append(c.replace(c,FA_D1))
                         #print(c)
                    elif c == Ar_D2 or c == EN_D2:
                         normalized_text.append(c.replace(c,FA_D2))
                    elif c == Ar_D3 or c == EN_D3:
                         normalized_text.append(c.replace(c,FA_D3))
                    elif c == Ar_D4 or c == EN_D4:
                         normalized_text.append(c.replace(c,FA_D4))
                    elif c == Ar_D5 or c == EN_D5:
                         normalized_text.append(c.replace(c,FA_D5))
                    elif c == Ar_D6 or c == EN_D6 :
                         normalized_text.append(c.replace(c,FA_D6))
                    elif c == Ar_D7 or c == EN_D7:
                         normalized_text.append(c.replace(c,FA_D7))
                    elif c == Ar_D8 or c ==EN_D8 :
                         normalized_text.append(c.replace(c,FA_D8))
                    elif c == Ar_D9 or c == EN_D9:
                         normalized_text.append(c.replace(c,FA_D9))
                    # =================================================================
                    elif c == "?":
                         c = "؟"
                         normalized_text.append(c)
                    else:
                         normalized_text.append(c)
          normalized_text = ''.join(normalized_text)
          normalized_text=normalized_text.split()
          suffix_words = {"ها", "های", "هایی", "هاای", "هایمان", "هایت", "هایش", "هایشان", "ات", "هایتان", "تان", "شان",
                          "شون", "تون", "مون", "شان", "مان", "ایی", "ای", "ی", "یی", "ام", "اد", "ایم", "اید", "اند",
                          "یم", "ید", "اندود", "پور", "خیز", "دهنده", "انگیز", "نشین", "نشینی", "آور", "جات", "ات",
                          "تا", "ین", "ان", "سازی", "آلود",
                          "آمیز", "کننده", "سوزی", "خوری", "گویان", "افکنی", "افکن", "دان", "دانی", "پرور", "شناسان",
                          "شناس", "پاش", "وند", "کننده", "زدایی", "کنندگی", "کنان", "گیری", "زاد", "سنج", "یاب", "گان",
                          "گی", "گر", "گیری", "یت", "ناک", "اک", "کان", "انه", "مند", "ور", "گین", "سار", "ار", "ی",
                          "شناس", "ۀ", "شناسی", "اباد", "گار", "گذار", "زی", "پرداز", "رسانی", "پژوه", "پیما", "نویس",
                          "کشان", "ترین", "تر", "پذیر", "جویی", "برداری", "ند",
                          }
          prefix = {"نا", "همی", "بی", "می", "نمی", "درمی", "درنمی", "فرا", "سو", "سوا", "برمی", "برنمی", "ا", "ابی",
                    "ال", "ابو"}
          Normtoken = list();
          addingtoken = ''
          j = 0
          for i in range(1, len(normalized_text) + 1):
               if j >= len(normalized_text):
                    break
               word = normalized_text[j]
               try:
                    if normalized_text[j] in prefix and normalized_text[j + 2] in suffix_words:
                         addingtoken = normalized_text[j] + normalized_text[j + 1] + normalized_text[j + 2]
                         Normtoken.append(addingtoken)
                         i = i + 3
                         continue;
               except:
                    None
               try:
                    if normalized_text[j] in prefix:
                         addingtoken = normalized_text[j] + normalized_text[j + 1]
                         Normtoken.append(addingtoken)
                         j = j + 2
                         continue;
               except:
                    None
               try:
                    if normalized_text[j + 1] in suffix_words:
                         addingtoken = normalized_text[j] + normalized_text[j + 1]
                         Normtoken.append(addingtoken)
                         j = j + 2
                         continue;
               except:
                    None
               Normtoken.append(normalized_text[j])
               j = j + 1

          result = ' '.join(word for word in Normtoken)
          # data={}
          # data["text"]=result
          return result

     def normalize_token(Token):
          '''
          :param Token: list of un normal token
          :return: Normal tokens
          '''
