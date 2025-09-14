from agents.rule_code_block_agent import RuleCodeBlockAgent

class BulkRuleSuggestionAgent:
    """
    Uses LLM to analyze user comments and suspected records, and suggest multiple rules with code_blocks.
    """
    def __init__(self):
        self.rule_agent = RuleCodeBlockAgent()

    def suggest_rules(self, comments, suspected_records):
        # For demo: generate a single rule based on comments. In production, use LLM to analyze and generate multiple rules.
        # You can expand this logic to use the ADK agent for more advanced suggestions.
        # Example: parse comments for field names, logic, etc.
        # Here, we just create a dummy rule for illustration.
        rules = []
        # TODO: Replace with LLM-driven logic
        rule = {
            "rule_name": "Suggested Rule from Comments",
            "description": comments,
            "source_field": "customer_email",
            "target_field": "email",
            "match_classification": "Match",
            "match_type": "Exact",
            "rationale_statement": "Suggested by agent based on user comments.",
        }
        code_block = self.rule_agent.generate_code_block(
            rule["description"],
            rule["rationale_statement"],
            rule["source_field"],
            rule["target_field"]
        )
        rule["code_block"] = code_block
        rules.append(rule)
        return rules
