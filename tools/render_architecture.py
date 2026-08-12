from PIL import Image, ImageDraw, ImageFont

W, H = 1100, 420
bg = (255,255,255)
fill = (240,248,255)
outline = (40,40,40)
arrow = (30,30,30)

img = Image.new('RGB',(W,H),bg)
d = ImageDraw.Draw(img)

try:
    font = ImageFont.truetype('DejaVuSans.ttf', 14)
except Exception:
    font = ImageFont.load_default()

# helper to draw a box with text centered
def draw_box(cx, cy, w, h, text):
    x0 = cx - w//2
    y0 = cy - h//2
    x1 = cx + w//2
    y1 = cy + h//2
    d.rectangle([x0,y0,x1,y1], fill=fill, outline=outline, width=2)
    # wrap text by newline
    lines = text.split('\n')
    _, th = d.textsize('Ay', font=font)
    total_h = th * len(lines)
    start_y = cy - total_h//2
    for i, line in enumerate(lines):
        tw, _ = d.textsize(line, font=font)
        d.text((cx - tw/2, start_y + i*th), line, fill=outline, font=font)
    return (x0,y0,x1,y1)

# helper to draw arrow from (x0,y0) to (x1,y1)
def draw_arrow(x0,y0,x1,y1):
    d.line((x0,y0,x1,y1), fill=arrow, width=3)
    # draw simple triangle
    import math
    angle = math.atan2(y1-y0, x1-x0)
    size = 10
    p1 = (x1, y1)
    p2 = (x1 - size*math.cos(angle - math.pi/6), y1 - size*math.sin(angle - math.pi/6))
    p3 = (x1 - size*math.cos(angle + math.pi/6), y1 - size*math.sin(angle + math.pi/6))
    d.polygon([p1,p2,p3], fill=arrow)

# nodes
nodes = {}
nodes['A'] = (120,70)
nodes['B'] = (320,70)
nodes['C'] = (520,70)
nodes['D'] = (520,230)
nodes['E'] = (760,230)
nodes['F'] = (920,230)
nodes['G'] = (760,330)

# labels (replace <br/> with newline)
labels = {
    'A': 'Python Generator',
    'B': 'data/incoming/',
    'C': 'Spark Structured\nStreaming',
    'D': 'transformations.py\ncast / validate / dedupe / derive',
    'E': 'PostgreSQL:\nevents table',
    'F': 'validation_queries.sql',
    'G': 'metrics.py\nbatch_metrics.csv'
}

# draw boxes
sizes = {
    'A': (220,60), 'B': (220,60), 'C': (220,70), 'D': (360,90), 'E': (260,70), 'F': (220,50), 'G': (260,70)
}
for k,p in nodes.items():
    w,h = sizes[k]
    draw_box(p[0], p[1], w, h, labels[k])

# draw arrows using simple offsets
arrow = (30,30,30)
# A -> B
draw_arrow(nodes['A'][0]+110, nodes['A'][1], nodes['B'][0]-110, nodes['B'][1])
# B -> C
draw_arrow(nodes['B'][0]+110, nodes['B'][1], nodes['C'][0]-110, nodes['C'][1])
# C -> D (down)
draw_arrow(nodes['C'][0], nodes['C'][1]+35, nodes['D'][0], nodes['D'][1]-45)
# D -> E
draw_arrow(nodes['D'][0]+180, nodes['D'][1], nodes['E'][0]-130, nodes['E'][1])
# E -> F
draw_arrow(nodes['E'][0]+130, nodes['E'][1], nodes['F'][0]-110, nodes['F'][1])
# D -> G
# from bottom of D to top of G with slight curve (we'll do two segments)
mid1 = (nodes['D'][0], nodes['D'][1]+45)
mid2 = (nodes['G'][0]-150, nodes['G'][1]-45)
d.line((mid1[0], mid1[1], mid2[0], mid2[1]), fill=arrow, width=3)
# arrow head into G
import math
ax,ay = nodes['G'][0]-120, nodes['G'][1]-45
angle = math.atan2(nodes['G'][1]-ay, nodes['G'][0]-ax)
size = 10
p1 = (nodes['G'][0]-40, nodes['G'][1]-45)
p2 = (p1[0] - size*math.cos(angle - math.pi/6), p1[1] - size*math.sin(angle - math.pi/6))
p3 = (p1[0] - size*math.cos(angle + math.pi/6), p1[1] - size*math.sin(angle + math.pi/6))
d.polygon([ (nodes['G'][0]-40, nodes['G'][1]-45), p2, p3 ], fill=arrow)

# save
img.save('docs/system_architecture.png')
print('Wrote docs/system_architecture.png')
