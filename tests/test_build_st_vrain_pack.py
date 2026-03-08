import unittest
from datetime import datetime

from scripts.build_st_vrain_pack import (
    DISTRICT_TZ,
    build_seed_source_urls,
    current_board_years,
    is_school_hub_url,
    specialized_page_text,
    should_exclude_url,
    should_include_discovered_url,
)


class BuildStVrainPackTests(unittest.TestCase):
    def test_current_board_years_uses_current_and_next_year(self) -> None:
        reference = datetime(2026, 1, 15, 12, 0, tzinfo=DISTRICT_TZ)
        self.assertEqual(current_board_years(reference), [2026, 2027])

    def test_seed_sources_include_dynamic_board_year_pages(self) -> None:
        reference = datetime(2026, 6, 1, 9, 0, tzinfo=DISTRICT_TZ)
        sources = build_seed_source_urls(reference)
        self.assertIn(
            "https://www.svvsd.org/about/board-of-education/board-meetings/2026-board-meetings/",
            sources,
        )
        self.assertIn(
            "https://www.svvsd.org/about/board-of-education/board-meetings/2027-board-meetings/",
            sources,
        )

    def test_exclude_staffmembers(self) -> None:
        self.assertTrue(
            should_exclude_url("https://www.svvsd.org/staffmembers/example-person/")
        )

    def test_exclude_login_pages(self) -> None:
        self.assertTrue(should_exclude_url("https://www.svvsd.org/login/"))

    def test_include_school_pages(self) -> None:
        self.assertTrue(
            should_include_discovered_url(
                "https://www.svvsd.org/school/alpine-elementary/",
                [2026, 2027],
            )
        )

    def test_exclude_old_board_archives(self) -> None:
        self.assertFalse(
            should_include_discovered_url(
                "https://www.svvsd.org/about/board-of-education/board-meetings/2023-board-meetings/",
                [2026, 2027],
            )
        )

    def test_include_current_board_pages(self) -> None:
        self.assertTrue(
            should_include_discovered_url(
                "https://www.svvsd.org/about/board-of-education/board-meetings/2026-board-meetings/",
                [2026, 2027],
            )
        )

    def test_school_hub_url_is_identified_for_exclusion_from_school_section(self) -> None:
        self.assertTrue(is_school_hub_url("https://www.svvsd.org/schools/"))
        self.assertFalse(is_school_hub_url("https://www.svvsd.org/schools/innovation-center/"))

    def test_specialized_board_cleanup_formats_members_cleanly(self) -> None:
        source = "\n".join(
            [
                "Members",
                "PRESIDENT",
                "Jocelyn Gilligan",
                "District: A",
                "Contact:",
                "phone",
                "email",
                "About",
                "Board biography text.",
            ]
        )
        cleaned = specialized_page_text(
            "https://www.svvsd.org/about/board-of-education/",
            source,
        )
        self.assertIn("Member: Jocelyn Gilligan", cleaned)
        self.assertIn("Role: President", cleaned)
        self.assertIn("Contact: phone | email", cleaned)
        self.assertIn("About: Board biography text.", cleaned)
        self.assertNotIn("Members", cleaned)

    def test_specialized_board_cleanup_splits_assistant_secretary_member(self) -> None:
        source = "\n".join(
            [
                "TREASURER",
                "Jacqueline",
                "About",
                "Treasurer biography.",
                "ASSISTANT SECRETARY",
                "Geno Lechuga",
                "District: G, Term: 2023 - 2027",
            ]
        )
        cleaned = specialized_page_text(
            "https://www.svvsd.org/about/board-of-education/",
            source,
        )
        self.assertIn("Member: Geno Lechuga", cleaned)
        self.assertIn("Role: Assistant Secretary", cleaned)
        self.assertNotIn("About: Treasurer biography. ASSISTANT SECRETARY", cleaned)

    def test_specialized_cde_profile_cleanup_removes_ui_labels(self) -> None:
        source = "\n".join(
            [
                "St Vrain Valley RE1J",
                "Directions",
                "32,279",
                "Total Students Served",
                "View School List",
                "Enrollment",
                "33,000",
                "School Year: 2025-2026",
            ]
        )
        cleaned = specialized_page_text(
            "https://www.cde.state.co.us/schoolview/explore/profile/0470",
            source,
        )
        self.assertIn("District: St Vrain Valley RE1J", cleaned)
        self.assertIn("Total Students Served: 32,279", cleaned)
        self.assertIn("Enrollment: 33,000 (School Year: 2025-2026)", cleaned)
        self.assertNotIn("Directions", cleaned)
        self.assertNotIn("View School List", cleaned)

    def test_specialized_cde_framework_cleanup_strips_leading_colons(self) -> None:
        source = "\n".join(
            [
                "Performance Frameworks - Official Performance Ratings",
                "St Vrain Valley RE1J (0470)",
                "Selected Report Year",
                ":",
                "2025",
                "Rating",
                ":",
                "Accredited",
                "Performance Watch Status",
                ":",
                "Not on Performance Watch",
                "Rating Source",
                ":",
                "Rating based on 1-Year Performance Report",
            ]
        )
        cleaned = specialized_page_text(
            "https://www.cde.state.co.us/schoolview/frameworks/official/0470",
            source,
        )
        self.assertIn("Selected Report Year: 2025", cleaned)
        self.assertIn("Rating: Accredited", cleaned)
        self.assertIn("Performance Watch Status: Not on Performance Watch", cleaned)
        self.assertIn("Rating Source: Rating based on 1-Year Performance Report", cleaned)
        self.assertNotIn(": :", cleaned)


if __name__ == "__main__":
    unittest.main()
