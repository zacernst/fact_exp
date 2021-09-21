"""
Just trying stuff.
"""

from ply import lex
import ply.yacc as yacc


tokens = (
    "NUMBER", "MATCH",
    "PLUS",
    "MINUS",
    "COMMA",
    "LPAREN",
    "RPAREN",
    "LCURLY",
    "RCURLY",
    "WHERE",
    "TRUE",
    "FALSE",
    "AND",
    "OR",
    "NOT",
    "NAME",
    "COLON",
    "EQUALS",
    "LEFT_RIGHT_ARROW_PLAIN",
    "RIGHT_LEFT_ARROW_PLAIN",
    "LSQUARE",
    "RSQUARE",
    "LEFT_ARROW_HEAD",
    "RIGHT_ARROW_HEAD",
)


t_PLUS = r"\+"
t_COLON = r":"
t_COMMA = r","
t_MINUS = r"-"
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_LCURLY = r"\{"
t_RCURLY = r"\}"
t_LSQUARE = r"\["
t_RSQUARE = r"\]"
t_WHERE = r"WHERE"
t_LEFT_RIGHT_ARROW_PLAIN = r"-->"
t_RIGHT_LEFT_ARROW_PLAIN = r"<--"
t_RIGHT_ARROW_HEAD = r"->"
t_LEFT_ARROW_HEAD = r"<-"


t_EQUALS = r"="


def t_NUMBER(t):
    r"\d+"
    t.value = int(t.value)
    return t


t_ignore = " \t"


def t_TRUE(t):
    r"TRUE"
    return t


def t_FALSE(t):
    r"FALSE"
    return t


def t_AND(t):
    r"AND"
    return t


def t_OR(t):
    r"OR"
    return t


def t_NOT(t):
    r"NOT"
    return t


def t_MATCH(t):
    r"MATCH"
    return t


def t_NAME(t):
    r"[A-Za-z]+"
    return t


lexer = lex.lex()


class Expression:
    """
    Top level thing
    """

    def __init__(self, ast, variable_mapping):
        self.ast = ast
        self.variable_mapping = variable_mapping

    def __repr__(self):
        out = "\n".join([str(self.ast), str(self.variable_mapping), "---"])
        return out


def p_expression(p):
    """expression : value
    | boolean
    | node_constraint
    | node
    | relationship_list
    | match
    """
    p[0] = Expression(p[1], getattr(p, "variable_mapping", {}))


class Variable:
    """Superclass for all variables"""

    def __init__(self, name: str = None):
        self.name = name

    def __repr__(self):
        return f"Variable({self.name})"


class Node:
    """
    A node.
    """

    def __init__(
        self, name: str = None, node_type: str = None, constraint=None
    ):
        self.name = name
        self.node_type = node_type
        self.constraint = constraint

    def __repr__(self):
        return f"Node({self.name}: {self.node_type} WHERE {self.constraint})"


class AtomicConstraint:
    """
    Simplest constraint.
    """

    def __init__(
        self, name: str = None, relation: str = None, value: float = None
    ):
        self.name = name
        self.relation = relation
        self.value = value

    def __repr__(self):
        return f"Constraint: {self.name} {self.relation}  {self.value}"


def p_constraint_subexpression(p):
    """constraint : NAME EQUALS value"""
    p[0] = AtomicConstraint(
        name=variable_factory(p, p[1]), relation="EQUALS", value=p[3]
    )


class ConstraintList:
    """
    A list of constraints
    """

    def __init__(self, constraint_list: list = None):
        self.constraint_list = constraint_list or []

    def __repr__(self):
        return "[" + ", ".join([str(i) for i in self.constraint_list]) + "]"


def p_node_constraint(p):
    """node_constraint : LCURLY constraint_list RCURLY"""
    p[0] = p[2]


def p_constraint_list(p):
    """constraint_list : constraint
    | constraint_list COMMA constraint"""
    if len(p) == 2:  # only one constraint
        constraint = p[1]
        constraint_list = ConstraintList(constraint_list=[constraint])
        p[0] = constraint_list
    elif len(p) == 4 and isinstance(p[1], (ConstraintList,)):
        p[1].constraint_list.append(p[3])
        p[0] = p[1]
    else:
        raise Exception("Unreachable")


def p_node(p):
    """node : LPAREN NAME RPAREN
    | LPAREN NAME COLON NAME RPAREN
    | LPAREN NAME COLON NAME node_constraint RPAREN"""
    name = variable_factory(p, p[2])
    node_type = None
    node_constraint = None
    if len(p) == 6:
        node_type = p[4]
    if len(p) == 7:
        node_type = p[4]
        node_constraint = p[5]
    node = Node(name=name, node_type=node_type, constraint=node_constraint)
    p[0] = node


def p_value(p):
    """value : NUMBER
    | LPAREN value RPAREN
    | value PLUS value
    | value MINUS value"""
    if len(p) == 2:
        p[0] = int(p[1])
    elif p[2] == "+":
        p[0] = int(p[1]) + int(p[3])
    elif p[2] == "-":
        p[0] = int(p[1]) - int(p[3])
    elif p[1] == "(" and p[3] == ")":
        p[0] = p[2]
    else:
        raise Exception("huh?")


def p_boolean(p):
    """boolean : TRUE
    | FALSE
    | LPAREN boolean AND boolean RPAREN
    | LPAREN boolean OR boolean RPAREN
    | NOT boolean
    """
    if len(p) == 2 and p[1] == "TRUE":
        p[0] = True
    elif len(p) == 2 and p[1] == "FALSE":
        p[0] = False
    elif len(p) == 6 and p[3] == "AND":
        p[0] = p[2] and p[4]
    elif len(p) == 6 and p[3] == "OR":
        p[0] = p[2] or p[4]
    elif len(p) == 3 and p[1] == "NOT":
        p[0] = not p[2]
    else:
        raise Exception("Unreachable?")


class RelationshipSpec:
    """The part in the square brackets"""

    def __init__(
        self, relationship_type: str = None, relationship_name: str = None
    ):
        self.relationship_name = relationship_name
        self.relationship_type = relationship_type

    def __repr__(self):
        out = f"[Name: {self.relationship_name}]"
        return out


class Relationship:
    """Basic relationship class. Not a chain."""

    def __init__(
        self,
        source: Node = None,
        target: Node = None,
        left_node: Node = None,
        right_node: Node = None,
        relationship_spec: RelationshipSpec = None,
    ):
        self.source = source
        self.target = target
        self.left_node = left_node
        self.right_node = right_node
        self.relationship_spec = relationship_spec

    def __repr__(self):
        out = f"{self.source} --> {self.target} SPEC: {self.relationship_spec}"
        return out


class RelationshipList:
    """A list of relationships."""

    def __init__(self, relationship_list: list = None):
        self.relationship_list = relationship_list or []

    def __repr__(self):
        return ", ".join([str(i) for i in self.relationship_list])


class Match:
    '''MATCH pattern without a WHERE clause'''
    def __init__(self, pattern):
        self.pattern = pattern


def p_match_clause(p):
    '''
    match_clause : MATCH pattern
    '''
    return Match(p[2])


class Pattern:
    '''A set of constraints without a WHERE clause'''
    def __init__(self, pattern_list: list = None):
        self.pattern_list = pattern_list or []


def p_pattern(p):  # Any pattern we have to match in the MATCH or MERGE clause
    '''
    pattern : relationship_list
    | node
    | pattern COMMA pattern
    '''
    pass
    


def p_relationship_list(p):  # A Relationship between nodes
    """relationship_list : node LEFT_RIGHT_ARROW_PLAIN node
    | node RIGHT_LEFT_ARROW_PLAIN node
    | node MINUS relationship_spec RIGHT_ARROW_HEAD node
    | node LEFT_ARROW_HEAD relationship_spec MINUS node
    | relationship_list LEFT_RIGHT_ARROW_PLAIN node
    | relationship_list RIGHT_LEFT_ARROW_PLAIN node"""

    relationship_obj = None
    if len(p) == 4 and p[2] == "-->" and isinstance(p[1], (Node,)):
        relationship_obj = Relationship(
            source=p[1], target=p[3], left_node=p[1], right_node=p[3]
        )
        relationship_obj = RelationshipList(
            relationship_list=[relationship_obj]
        )
    elif len(p) == 4 and p[2] == "<--" and isinstance(p[1], (Node,)):
        relationship_obj = Relationship(
            source=p[3], target=p[1], left_node=p[1], right_node=p[3]
        )
        relationship_obj = RelationshipList(
            relationship_list=[relationship_obj]
        )
    elif len(p) == 6 and p[2] == "-" and isinstance(p[1], (Node,)):
        relationship_obj = Relationship(
            source=p[1],
            target=p[5],
            relationship_spec=p[3],
            left_node=p[1],
            right_node=p[5],
        )
        relationship_obj = RelationshipList(
            relationship_list=[relationship_obj]
        )
    elif len(p) == 6 and p[2] == "<-" and isinstance(p[1], (Node,)):
        relationship_obj = Relationship(
            source=p[5],
            target=p[1],
            relationship_spec=p[3],
            left_node=p[1],
            right_node=p[5],
        )
        relationship_obj = RelationshipList(
            relationship_list=[relationship_obj]
        )
    elif (
        len(p) == 4 and p[2] == "-->" and isinstance(p[1], (RelationshipList,))
    ):
        relationship_obj = Relationship(
            source=p[1].relationship_list[-1].right_node,
            target=p[3],
            left_node=p[1].relationship_list[-1].right_node,
            right_node=p[3],
        )
        p[1].relationship_list.append(relationship_obj)
        relationship_obj = p[1]
    elif (
        len(p) == 4 and p[2] == "<--" and isinstance(p[1], (RelationshipList,))
    ):
        relationship_obj = Relationship(
            source=p[3],
            target=p[1].relationship_list[-1].right_node,
            left_node=p[1].relationship_list[-1].right_node,
            right_node=p[3],
        )
        p[1].relationship_list.append(relationship_obj)
        relationship_obj = p[1]
    else:
        raise Exception("Unreachable in relationship")
    p[0] = relationship_obj


def variable_factory(p, variable_name):
    """
    Create a variable if it doesn't exist already; return it.
    """
    if isinstance(variable_name, (Variable,)):
        return variable_name
    if not hasattr(p, "variable_mapping"):
        setattr(p, "variable_mapping", {})
    if variable_name not in p.variable_mapping:
        p.variable_mapping[variable_name] = Variable(variable_name)
    return p.variable_mapping[variable_name]


def p_relationship_spec(p):
    """relationship_spec : LSQUARE NAME RSQUARE
    | LSQUARE NAME COLON NAME RSQUARE"""
    if len(p) == 4:
        p[2] = variable_factory(p, p[2])
        relationship_spec = RelationshipSpec(
            relationship_name=variable_factory(p, p[2])
        )
    elif len(p) == 6:
        relationship_spec = RelationshipSpec(
            relationship_type=p[4], relationship_name=variable_factory(p, p[2])
        )
    else:
        raise Exception("unreachable in relationship_spec")
    p[0] = relationship_spec


class Match:
    '''A match'''
    def __init__(self, relationship_list: list = None):
        self.relationship_list = relationship_list or []

    def __repr__(self):
        out = f'MATCH: {self.relationship_list}'
        return out


def p_match(p):
    '''match : MATCH relationship_list'''
    p[0] = Match(relationship_list=p[2])


parser = yacc.yacc()
result = parser.parse("((1 + 1) + 10)")
print(result)
result = parser.parse("(TRUE OR (TRUE AND FALSE))")
print(result)
result = parser.parse("(thing:foo)")
print(result)
result = parser.parse("(THING:FOO {Bar = ((2 + 2) - 1), Baz = 1})")
print(result)
result = parser.parse("(THING: Foo)-->(OTHER: Whatever)")
print(result)
result = parser.parse("(THING: Foo)<--(OTHER: Whatever)")
print(result)
result = parser.parse("(THING: Foo)-[r]->(OTHER: Whatever)")
print(result)
result = parser.parse("(thing: foo)<-[r]-(other: whatever)")
print(result)
result = parser.parse("(thing: foo)<-[r: IMARELATIONSHIP]-(other: whatever)")
print(result)
result = parser.parse(
    "(thing: foo)<-[r: imarelationship]-(other: whatever)-->(p)"
)
print(result)
result = parser.parse("(a)-->(b)-->(c)<--(d)")
print(result)
result = parser.parse("MATCH (a)-->(b)-->(c)<--(d)")
print(result)

if __name__ == "__main__":
    pass
