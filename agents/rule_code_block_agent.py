import os
import asyncio
from dotenv import load_dotenv

from google.adk.agents.llm_agent import LlmAgent
from google.genai.types import GenerateContentConfig
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService

class RuleCodeBlockAgent:
    def _ensure_api_key(self):
        # Load .env and set GOOGLE_API_KEY if not already set
        load_dotenv()
        if not os.environ.get("GOOGLE_API_KEY"):
            raise RuntimeError("GOOGLE_API_KEY is not set. Please check your .env file.")
    """
    ADK LLM Agent for generating and validating Python code blocks for rule logic.
    """
    def __init__(self, model="gemini-2.0-flash"):
        self._ensure_api_key()
        self.agent = LlmAgent(
            model=model,
            name="rule_code_block_agent",
            description="Generates and validates Python code blocks for data matching rules.",
            instruction=self._build_instruction(),
            generate_content_config=GenerateContentConfig(
                temperature=0.2,
                max_output_tokens=300
            )
        )
        self.session_service = InMemorySessionService()
        self.runner = Runner(agent=self.agent, app_name="rule_code_block_app", session_service=self.session_service)

    def _build_instruction(self):
        return (
            "You are an expert Python developer. Given a rule description, rationale statement, "
            "source field, and target field, generate a Python function named 'rule_code_block' "
            "that takes two arguments: source_value and target_value. The function should return True "
            "if the rule matches, otherwise False. Respond ONLY with the function code, no explanation."
        )

    async def async_generate_code_block(self, description, rationale_statement, source_field, target_field):
        prompt = (
            f"Rule Description: {description}\n"
            f"Rationale Statement: {rationale_statement}\n"
            f"Source Field: {source_field}\n"
            f"Target Field: {target_field}\n"
        )
        user_id = "test_user"
        session_id = "test_session"
        await self.session_service.create_session(
            app_name="rule_code_block_app", user_id=user_id, session_id=session_id
        )
        from google.genai import types
        content = types.Content(role='user', parts=[types.Part(text=prompt)])
        events = self.runner.run(user_id=user_id, session_id=session_id, new_message=content)
        final_answer = None
        for event in events:
            if hasattr(event, 'is_final_response') and event.is_final_response() and event.content:
                final_answer = event.content.parts[0].text.strip()
        if not final_answer:
            raise RuntimeError("No final response from agent.")
        code = final_answer
        if code.startswith('```'):
            code = code.split('```')[1]
            if code.startswith('python'):
                code = code[len('python'):].strip()
        return code.strip()

    def generate_code_block(self, description, rationale_statement, source_field, target_field):
        # Synchronous wrapper for compatibility
        return asyncio.run(self.async_generate_code_block(description, rationale_statement, source_field, target_field))
