import pymupdf as pdf
import random
import numpy
import math
import re

# how similiar positions should be to each other to count as the same
DIFF = 0.01
# how much of the page should be checked for header/footer lines
LINE_FRAC = 0.3
# should none be found,
# how much of the page should be checked for headers and footers
SEC_FRAC = 0.125


# class to make sure vectorising works correctly
# and doesn't see groups of two blocks as another dimension
class compBlock:
    def __init__(self, block1: dict, block2: dict):
        self.block1 = block1
        self.block2 = block2


# function for returning an object based on a boolean
def zeroIfFalse(obj, val: bool):
    if val:
        return obj
    else:
        return 0


zeroIfFalse = numpy.frompyfunc(zeroIfFalse, 2, 1)


def isSimiliarBlock(obj: compBlock) -> bool:
    # done by checking if the two blocks share a x-edge and a y-edge,
    # or have similiar fonts

    # check x coords
    b1Spans = {(span["color"], span["font"], span["size"]) for
               lines in obj.block1["lines"] for span in lines["spans"]}
    b2Spans = {(span["color"], span["font"], span["size"]) for
               lines in obj.block2["lines"] for span in lines["spans"]}
    b1Text = "".join([span["text"] for lines in obj.block1["lines"] for
                      span in lines["spans"]])
    b2Text = "".join([span["text"] for lines in obj.block2["lines"] for
                      span in lines["spans"]])
    if (abs(obj.block1["bbox"][0] - obj.block2["bbox"][0]) < DIFF or
            abs(obj.block1["bbox"][2] - obj.block2["bbox"][2]) < DIFF):
        # check y coords
        if (abs(obj.block1["bbox"][1] - obj.block2["bbox"][1]) < DIFF or
                abs(obj.block1["bbox"][3] - obj.block2["bbox"][3]) < DIFF):
            if (re.sub(r"\d", r"", b1Text) == re.sub(r"\d", r"", b2Text)):
                return True
            # check font
            if b1Spans == b2Spans:
                return True
    return False


isSimiliarBlock = numpy.frompyfunc(isSimiliarBlock, 1, 1)


def isSimiliarLine(obj: compBlock) -> bool:
    return obj.block1["rect"] == obj.block2["rect"]


isSimiliarLine = numpy.frompyfunc(isSimiliarLine, 1, 1)


def removeHeaderFooter(doc: pdf.Document, pageCount=15) -> pdf.Document:
    pageCount = min(pageCount, len(doc)-1)
    # get random sequence of 15 pages
    start = max(1, random.randint(1, max(len(doc)-15, 1)))
    end = min(start + pageCount, len(doc))
    pages: list[pdf.Page] = doc[start:end]

    # check for header and footer lines
    possibleHeaderLines = []
    possibleFooterLines = []
    for page in pages:
        # get all drawings that are longer than they are tall
        lines = [line for line in page.get_cdrawings() if
                 abs(line["rect"][0] - line["rect"][2]) >
                 abs(line["rect"][1] - line["rect"][3])]
        headerLines = [line for line in lines if
                       line["rect"][3] < page.bound().height*LINE_FRAC]
        possibleHeaderLines = possibleHeaderLines + headerLines
        footerLines = [line for line in lines if
                       line["rect"][1] > page.bound().height*(1-LINE_FRAC)]
        possibleFooterLines = possibleFooterLines + footerLines

    # create matrices for comparison
    headerLineMatrix = [[compBlock(row, col) for col in possibleHeaderLines]
                        for row in possibleHeaderLines]
    footerLineMatrix = [[compBlock(row, col) for col in possibleFooterLines]
                        for row in possibleFooterLines]
    headerLineArray = numpy.array(headerLineMatrix)
    footerLineArray = numpy.array(footerLineMatrix)

    # get which lines are the same across multiple pages
    headerLinePosArray = [isSimiliarLine(obj) for obj in headerLineArray]
    footerLinePosArray = [isSimiliarLine(obj) for obj in footerLineArray]

    # get how many pages each line is on, follows the following formula:
    #       (n-1)n
    #  x = --------
    #         2
    # where x is the resulting count, and n is the number of pages
    # only need to check the bottom triangle of the comparrison matrix
    checkingHeaderLineIndices = numpy.tril(headerLinePosArray, -1)
    checkingHeaderLines = zeroIfFalse(headerLineArray,
                                      checkingHeaderLineIndices)
    # get position values for similiar blocks
    badHeaderLines = [line.block1["rect"] for line in
                      checkingHeaderLines.flatten() if line]

    badHeaderLineDict = dict()
    for line in badHeaderLines:
        badHeaderLineDict[line] = badHeaderLineDict.get(line, 0) + 1

    # only need to check the bottom triangle of the comparrison matrix
    checkingFooterLineIndices = numpy.tril(footerLinePosArray, -1)
    checkingFooterLines = zeroIfFalse(footerLineArray,
                                      checkingFooterLineIndices)
    # get position values for similiar blocks
    badFooterLines = [line.block1["rect"] for line in
                      checkingFooterLines.flatten() if line]

    badFooterLineDict = dict()
    for line in badFooterLines:
        badFooterLineDict[line] = badFooterLineDict.get(line, 0) + 1

    # use the above formua to check if the lines are on most of the pages.
    # last bit is to ensure that missing a single line on one page
    # doesn't break recognition of it as a header/footer line
    n = pageCount-math.ceil(pageCount/10)
    badHeaderLines = [line for line in badHeaderLineDict.keys() if
                      badHeaderLineDict[line] >= (n-1)*n/2]
    badFooterLines = [line for line in badFooterLineDict.keys() if
                      badFooterLineDict[line] >= (n-1)*n/2]

    headerMax = False
    if badHeaderLines:
        # get the header bar closest to the top of the page
        badHeaderLines.sort(key=lambda x: x[1])
        headerMax = badHeaderLines[0][1]

    footerMin = False
    if badFooterLines:
        # get the footer bar closest to the bottom of the page
        badFooterLines.sort(key=lambda x: x[3])
        footerMin = badFooterLines[-1][3]

    possibleHeaderBlocks = []
    possibleFooterBlocks = []
    # get first and last 5 blocks of each page that are also in the search area
    for page in pages:
        if not headerMax:
            # no header bar was found, search smaller area
            headerMax = page.bound().height*SEC_FRAC
        if not footerMin:
            # no footer bar was found, search smaller area
            footerMin = page.bound().height*(1-SEC_FRAC)

        pageDict = page.get_text("dict")
        blocks = [block for block in pageDict["blocks"] if "lines" in block]
        possibleHeaders = blocks[:min(5, len(blocks))]
        possibleFooters = blocks[max(-5, -len(blocks)):]
        possibleHeaderFooters = possibleHeaders + possibleFooters
        newHeaders = [block for block in possibleHeaderFooters if
                      block["bbox"][3] < headerMax]
        possibleHeaderBlocks = possibleHeaderBlocks + newHeaders
        newFooters = [block for block in possibleHeaderFooters if
                      block["bbox"][1] > footerMin]
        possibleFooterBlocks = possibleFooterBlocks + newFooters

    # create matrices for comparison
    headerMatrix = [[compBlock(row, col) for col in possibleHeaderBlocks]
                    for row in possibleHeaderBlocks]
    footerMatrix = [[compBlock(row, col) for col in possibleFooterBlocks]
                    for row in possibleFooterBlocks]
    headerArray = numpy.array(headerMatrix)
    footerArray = numpy.array(footerMatrix)

    # determine how similiar each block is in position,
    # true for blocks that share the same position across multiple pages.
    headerPosArray = [isSimiliarBlock(obj) for obj in headerArray]
    footerPosArray = [isSimiliarBlock(obj) for obj in footerArray]

    # get how many pages share the same block. Uses the same formula from above
    # only need to check the bottom triangle of the comparrison matrix
    checkingHeaderIndices = numpy.tril(headerPosArray, -1)
    checkingHeaders = zeroIfFalse(headerArray, checkingHeaderIndices)
    # get position values for similiar blocks
    badHeaderBlocks = [block.block1["bbox"][0:4] for
                       block in checkingHeaders.flatten() if block]

    badHeaderDict = dict()
    for block in badHeaderBlocks:
        badHeaderDict[block] = badHeaderDict.get(block, 0) + 1

    # only need to check the bottom triangle of the comparrison matrix
    checkingFooterIndices = numpy.tril(footerPosArray, -1)
    checkingFooters = zeroIfFalse(footerArray, checkingFooterIndices)
    # get position values for similiar blocks
    badFooterBlocks = [block.block1["bbox"][0:4] for
                       block in checkingFooters.flatten() if block]

    badFooterDict = dict()
    for block in badFooterBlocks:
        badFooterDict[block] = badFooterDict.get(block, 0) + 1

    # should be more than half the pages to count (- safety margin)
    n = math.floor(pageCount/2.2)-math.ceil(pageCount/10)
    badHeaderBlocks = [block for block in badHeaderDict.keys()
                       if badHeaderDict[block] > (n-1)*n/2]
    badFooterBlocks = [block for block in badFooterDict.keys()
                       if badFooterDict[block] > (n-1)*n/2]

    if badHeaderBlocks:
        # get lowest y val
        badHeaderBlocks.sort(key=lambda x: x[3], reverse=True)
        headerBottom = badHeaderBlocks[0][3]

    if badFooterBlocks:
        # get highest y val
        badFooterBlocks.sort(key=lambda x: x[1])
        footerTop = badFooterBlocks[0][1]

    for page in doc:
        if badHeaderBlocks:
            rect = pdf.Rect((0, 0),
                            (page.bound().width,
                             headerBottom))
            page.add_redact_annot(rect)
        if badFooterBlocks:
            rect = pdf.Rect((0, footerTop),
                            (page.bound().width,
                             page.bound().height))
            page.add_redact_annot(rect)
        page.apply_redactions()

    return doc


def extractText(doc: pdf.Document) -> str:
    # initialise list of all blocks containing text
    blockList = []
    # move through doc page by page
    for page in doc:
        # get page contents
        pageDict = page.get_text("dict")
        # we're only interested in blocks that have text
        # (i.e. have "lines" key)
        blocks = [block for block in pageDict["blocks"] if "lines" in block]
        # for each block
        for block in blocks:
            b = block["bbox"]
            rect = pdf.Rect((b[0], b[1]), (b[2], b[3]))
            lines = block["lines"]
            # are there multiple lines in the block
            # (note, lines of text, not line objects)
            breaksSet = set()
            if abs(b[3]-lines[0]["bbox"][3]) > 0.001:
                # potential breaks are where lines start
                # (sections of text, not actual lines)
                breaks = [math.floor(line["bbox"][0]*10)/10 for line in lines]
                # remove duplicates
                breaksSet.update(breaks)
                # remove the start of the block, we don't need to check this.
                breaksSet.discard(math.floor(b[0]*10)/10)

                # do any potential breaks intersect with text?
                # if so, remove them
                breaks = list(breaksSet)
                for br in breaks:
                    for line in block["lines"]:
                        if br > line["bbox"][0] and br < line["bbox"][2]:
                            breaksSet.remove(br)
                            break

                # line breaks should only occur where there are abnormal gaps
                # i.e. tabs between words
                # however, these aren't contained in the pdf.
                # So whe need to find them ourselves.
                # if there are no breaks, we don't need to check though.
                spaces = []
                if len(breaksSet) > 0:
                    # get all words in the block
                    words = page.get_text("words", clip=rect)
                    # is there more than one word?
                    if len(words) > 1:
                        tempSpaces = []
                        for i in range(len(words[1:])):
                            # when comparing wih previous word:
                            # is it to the right?
                            # and is it on roughly the same line?
                            # check by centerline for abnormal chars:
                            # e.g. chars that extend lower, j, g etc
                            w1 = words[i]
                            w2 = words[i-1]
                            w1Line = (w1[1] + w1[3]) / 2
                            w2Line = (w2[1] + w2[3]) / 2
                            if w1[0] > w2[2] and abs(w1Line - w2Line) < 0.1:
                                tempSpaces.append((w2[2],
                                                   w1[1],
                                                   w1[0],
                                                   w2[3]))

                        # get the average space length
                        sum = 0.0
                        for s in tempSpaces:
                            sum = sum + (s[2]-s[0])
                        avg = sum/max(len(tempSpaces), 1)
                        # get all spaces that are abnormally long
                        for s in tempSpaces:
                            if s[2]-s[0] > avg*1.5:
                                spaces.append(s)

                # make sure each potential break intersects an abnormal space
                breaks = list(breaksSet)
                for br in breaks:
                    for s in spaces:
                        if br > s[0] and br < s[2]:
                            break
                    else:
                        breaksSet.remove(br)
                        continue
                    break

            # doubly make sure that the start and end of the block
            # aren't in the breaks set
            breaksSet.discard(b[0])
            breaksSet.discard(b[2])

            # get text in each section, from start to break 1,
            # then break 1 to break 2, etc
            # all the way to the end of the block
            start = b[0]
            for br in sorted(breaksSet):
                blockList.append(
                    page.get_text("text", clip=pdf.Rect(start,
                                                        b[1],
                                                        br,
                                                        b[3]))
                )
                start = br
            blockList.append(
                page.get_text("text", clip=pdf.Rect(start,
                                                    b[1],
                                                    b[2],
                                                    b[3]))
            )

    # convert list of text from blocks into a single string
    outString = '\n'.join(blockList)

    # remove extra spaces and non space characters
    outString = re.sub(r"[\s\a\u2003]+", r" ", outString)

    # write to file
    return outString


def process(path: str) -> dict[str, dict, set]:
    # import spacy here so machines without it can use the other functions
    import spacy
    with pdf.open(path) as inDoc:
        # get file name (and b64 encode it for later)
        name = path.split('/')[-1]
        # get file type
        fType = path.split('.')[-1].lower()
        # remove suffixes and filetype from file name for entity creation
        fileName = name.removesuffix(f".{fType}")
        fileId = re.search(r"_\d+$", fileName, re.MULTILINE)
        if fileId:
            fileName = fileName.removesuffix(fileId.group(0))
        # remove all headers and footers
        outDoc = removeHeaderFooter(inDoc)
        # extract the text
        text = extractText(outDoc)
        # do spacy processing
        nlp = spacy.load("en_LRVSP_spacy")
        doc = nlp(text)
        links = {ent.text.removeprefix("the ") for ent in doc.ents
                 if ent.label_ == "ref_doc"
                 and 4*math.ceil((len(ent.text)/3)) < 255}

        retDict = {
            "name": fileName,
            "metadata": dict(),
            "links": links
        }

        return retDict
